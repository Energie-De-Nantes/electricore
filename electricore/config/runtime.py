"""Registre runtime : lecteur unique de la configuration d'exécution (ADR-0024/0025, #141).

Un `BaseSettings` pydantic-settings indépendant par domaine (sftp, aes, duckdb,
api, bot, odoo), mappé sur les noms `<DOMAINE>__<CHAMP>` (convention ADR-0046 ;
ex. `BOT__TOKEN`, `DUCKDB__PATH`, `API__TITLE`). Deux sources, dans cet ordre de
précédence : env-système > `.env` à la racine
du dépôt. Accès par accessors de module mis en cache ; `valider(*accessors)`
offre le fail-fast par point d'entrée.
"""

import re
import secrets as _secrets
from functools import lru_cache
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigurationManquante(ValueError):
    """Variables d'environnement manquantes — ou présentes mais malformées —, groupées par domaine.

    Sous-classe `ValueError` pour rester compatible avec les `except ValueError`
    historiques (façade `charger_config_odoo`). `invalides` porte les messages des
    valeurs présentes mais rejetées par un validateur (ex. `ODOO__URL` mal formé,
    #454) — sinon le motif précis se perdrait derrière un simple « manquante ».
    """

    def __init__(self, manquantes: dict[str, list[str]], invalides: list[str] | None = None):
        self.manquantes = manquantes
        self.invalides = invalides or []
        details = " ; ".join(f"[{domaine}] {', '.join(noms)}" for domaine, noms in manquantes.items())
        message = f"Configuration manquante : {details}"
        if self.invalides:
            message += " | invalides : " + " ; ".join(self.invalides)
        super().__init__(message)


def _nom_var(prefixe: str, loc: tuple) -> str:
    """Nom de la variable d'environnement pour une erreur pydantic (loc → VAR)."""
    return prefixe + "__".join(str(part).upper() for part in loc)


def _instancier(domaine: str, classe: type[BaseSettings], prefixe: str, **kwargs) -> BaseSettings:
    """Instancie un domaine, traduisant les champs manquants en ConfigurationManquante."""
    try:
        return classe(_env_file=FICHIER_ENV, **kwargs)
    except ValidationError as exc:
        noms = [_nom_var(prefixe, err["loc"]) for err in exc.errors()]
        invalides = [err["msg"] for err in exc.errors() if err["type"] != "missing"]
        raise ConfigurationManquante({domaine: noms}, invalides=invalides) from exc


# Fichier .env ancré sur la racine du dépôt (None = désactivé, seam de test).
FICHIER_ENV: Path | None = Path(__file__).parents[2] / ".env"

# Base de prod locale, ancrée sur le package (jamais sur le CWD) — reprend env.py (#146).
_DEFAUT_BASE_DUCKDB = Path(__file__).parents[1] / "ingestion" / "flux_enedis_pipeline.duckdb"


class Duckdb(BaseSettings):
    """Domaine duckdb : chemin de la base de production."""

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")

    chemin: Path = Field(default=_DEFAUT_BASE_DUCKDB, validation_alias="DUCKDB__PATH")


_SCHEMAS_SFTP = re.compile(r"^(sftp|file|https?)://")


class Sftp(BaseSettings):
    """Domaine sftp : source des flux Enedis."""

    model_config = SettingsConfigDict(env_prefix="SFTP__", populate_by_name=True, extra="ignore")

    url: str

    @field_validator("url")
    @classmethod
    def _valider_schema(cls, v: str) -> str:
        if not _SCHEMAS_SFTP.match(v):
            raise ValueError(f"schéma non supporté (attendu sftp://, file://, http(s)://) : {v!r}")
        return v


_HEX_32_OR_64 = re.compile(r"^[0-9a-fA-F]{32}$|^[0-9a-fA-F]{64}$")


class PaireCles(BaseSettings):
    """Une clé AES et son IV **optionnel**, en hexadécimal.

    IV présent → schéma **IV-fixe** (AES-128 legacy). IV absent → schéma **IV-préfixé**
    (AES-256, ADR-0040) : l'IV est lu dans les 16 premiers octets de chaque fichier.
    """

    model_config = SettingsConfigDict(extra="ignore")

    key: str
    iv: str | None = None

    @field_validator("key", "iv")
    @classmethod
    def _valider_hex(cls, v: str | None) -> str | None:
        if v is not None and not _HEX_32_OR_64.match(v):
            raise ValueError(f"doit être un hexadécimal de 32 ou 64 caractères (reçu: {len(v)} car.)")
        return v

    def octets(self, etiquette: str) -> tuple[bytes, bytes | None]:
        try:
            iv = bytes.fromhex(self.iv) if self.iv is not None else None
            return bytes.fromhex(self.key), iv
        except ValueError as e:
            raise ValueError(f"Clé/IV invalides pour AES__{etiquette.upper()} (hexadécimal attendu) : {e}") from e


class Aes(BaseSettings):
    """Domaine aes : trousseau de clés de déchiffrement des flux (ADR-0037).

    Trousseau de taille arbitraire alimenté par `AES__TROUSSEAU__<label>__KEY` /
    `__IV`. Le `<label>` est un nom parlant choisi par l'opérateur (`aes256_2026`,
    `aes128_2024`…) qui remonte tel quel dans `chaine()`, donc dans les logs. La
    sélection de la bonne clé se fait par essai (cf. `decrypt_with_key_chain`),
    sans date ni protocole. Rupture de format assumée : `current`/`previous`/plat-v1
    d'ADR-0008 sont retirés (instance unique, ADR-0015).
    """

    model_config = SettingsConfigDict(
        env_prefix="AES__", env_nested_delimiter="__", populate_by_name=True, extra="ignore"
    )

    trousseau: dict[str, PaireCles] = Field(default_factory=dict)

    def chaine(self) -> list[tuple[str, bytes, bytes | None]]:
        """Trousseau aplati en [(label, clé, iv|None), …] — ordre indifférent (sélection par essai).

        `iv=None` dénote le schéma IV-préfixé (ADR-0040) ; le routage par schéma vit dans
        `decrypt_with_key_chain`.
        """
        return [(label, *paire.octets(label)) for label, paire in self.trousseau.items()]


def _dequote(valeur: str) -> str:
    """Trim + retire une paire de guillemets appariés (parité python-dotenv).

    En prod, `sops exec-env` exporte les valeurs dotenv **verbatim**, guillemets
    compris — là où le `.env` lu en dev passe par python-dotenv qui les retire. Sans
    rattrapage, un `ODOO__DB="edn"` (guillemets dans le secret) atteint `authenticate()`
    tel quel et Postgres répond `database ""edn"" does not exist`. On réplique donc la
    sémantique dotenv pour tout le bloc `ODOO__*` (#454 ne l'avait fait que pour l'URL).
    """
    nettoyee = valeur.strip()
    if len(nettoyee) >= 2 and nettoyee[0] == nettoyee[-1] and nettoyee[0] in "\"'":
        nettoyee = nettoyee[1:-1].strip()
    return nettoyee


def _normaliser_url(valeur: str, *, var: str) -> str:
    """Dé-quote (cf. `_dequote`) + exige un schéma http(s) (#454).

    Un `ODOO__URL="https://…"` non nettoyée atteint `ServerProxy` tel quel : `urlsplit`
    lit le schéma `"https` ≠ `http`/`https` et lève « unsupported XML-RPC protocol »
    (503 cryptique). On rejette en plus tôt un schéma absent avec un message clair.
    """
    url = _dequote(valeur)
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"{var} doit commencer par http:// ou https:// (reçu : {valeur!r})")
    return url


class Odoo(BaseSettings):
    """Domaine odoo : connexion XML-RPC read-only (ADR-0012), bloc unique ODOO__*.

    Plus de sélecteur test/prod (#439, #190 clos completed) : une seule base Odoo
    configurée, lue sous le préfixe `ODOO__` (convention ADR-0046 §5).
    """

    model_config = SettingsConfigDict(env_prefix="ODOO__", populate_by_name=True, extra="ignore")

    url: str
    db: str
    username: str
    password: str

    @field_validator("url")
    @classmethod
    def _url_normalisee(cls, valeur: str) -> str:
        return _normaliser_url(valeur, var="ODOO__URL")

    @field_validator("db", "username", "password")
    @classmethod
    def _identifiants_dequote(cls, valeur: str) -> str:
        return _dequote(valeur)


def _version_package() -> str:
    """Version du package electricore (suit pyproject.toml / tag de release)."""
    try:
        return _pkg_version("electricore")
    except PackageNotFoundError:
        return "0.0.0+unknown"


class CleConsommateur(BaseSettings):
    """Clé d'un consommateur de l'API, dans le trousseau (ADR-0046 §4)."""

    model_config = SettingsConfigDict(extra="ignore")

    key: str

    @field_validator("key")
    @classmethod
    def _valider_longueur(cls, v: str) -> str:
        if len(v) < 32:
            raise ValueError(f"doit contenir au moins 32 caractères (reçu: {len(v)})")
        return v


class Api(BaseSettings):
    """Domaine api : identité de l'instance et trousseau de clés des consommateurs.

    Trousseau étiqueté `API__TROUSSEAU__<consommateur>__KEY` (ADR-0046 §4, calqué sur
    le trousseau AES) : une clé par consommateur → révocation ciblée + attribution.
    """

    model_config = SettingsConfigDict(
        env_prefix="API__", env_nested_delimiter="__", populate_by_name=True, extra="ignore"
    )

    titre: str = Field(default="ElectriCore API", validation_alias="API__TITLE")
    version: str = Field(default_factory=_version_package, validation_alias="API__VERSION")
    description: str = Field(
        default="API sécurisée pour accéder aux données flux Enedis", validation_alias="API__DESCRIPTION"
    )
    instance_slug: str = Field(default="", validation_alias="INSTANCE_SLUG")
    trousseau: dict[str, CleConsommateur] = Field(default_factory=dict)

    def cles_valides(self) -> list[str]:
        """Clés acceptées : une par consommateur du trousseau (API__TROUSSEAU__<c>__KEY)."""
        return [c.key for c in self.trousseau.values() if c.key.strip()]

    def cle_valide(self, cle: str) -> bool:
        """Comparaison en temps constant (anti-timing), False sans clé configurée."""
        if not cle:
            return False
        return any(_secrets.compare_digest(cle, valide) for valide in self.cles_valides())

    def consommateur_pour(self, cle: str) -> str | None:
        """Label du consommateur dont la clé matche (attribution, ADR-0046 §4), sinon None."""
        if not cle:
            return None
        for label, consommateur in self.trousseau.items():
            if consommateur.key and _secrets.compare_digest(cle, consommateur.key):
                return label
        return None


class Bot(BaseSettings):
    """Domaine bot : Telegram uniquement (API_BASE_URL supprimée, ADR-0025)."""

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")

    token: str = Field(validation_alias="BOT__TOKEN")
    allowed_users: str = Field(default="", validation_alias="BOT__ALLOWED_USERS")
    notify_chat_id: str = Field(default="", validation_alias="BOT__NOTIFY_CHAT_ID")

    def utilisateurs_autorises(self) -> set[int]:
        """Allowlist Telegram : IDs entiers, les entrées invalides sont ignorées."""
        return {int(uid.strip()) for uid in self.allowed_users.split(",") if uid.strip().isdigit()}


@lru_cache
def duckdb() -> Duckdb:
    return _instancier("duckdb", Duckdb, "")


@lru_cache
def sftp() -> Sftp:
    return _instancier("sftp", Sftp, "SFTP__")


@lru_cache
def aes() -> Aes:
    domaine = _instancier("aes", Aes, "AES__")
    if not domaine.chaine():
        raise ConfigurationManquante(
            {"aes": ["AES__TROUSSEAU__<label>__KEY (IV optionnel : __IV = schéma IV-fixe, absent = IV-préfixé)"]}
        )
    return domaine


@lru_cache
def odoo() -> Odoo:
    """Connexion Odoo (bloc unique ODOO__*, read-only). Absente ⇒ ConfigurationManquante."""
    return _instancier("odoo", Odoo, "ODOO__")


@lru_cache
def api() -> Api:
    return _instancier("api", Api, "")


@lru_cache
def bot() -> Bot:
    return _instancier("bot", Bot, "")


_ACCESSORS = (duckdb, sftp, aes, odoo, api, bot)


def valider(*accessors) -> None:
    """Fail-fast d'un point d'entrée : appelle chaque accessor, agrège les manquantes.

    Contrats (ADR-0025) : API → valider(duckdb, api) ; bot → valider(bot) ;
    runner d'ingestion → valider(sftp, aes, duckdb) ; notebooks → rien d'imposé.
    """
    manquantes: dict[str, list[str]] = {}
    for accessor in accessors:
        try:
            accessor()
        except ConfigurationManquante as exc:
            for domaine, noms in exc.manquantes.items():
                manquantes.setdefault(domaine, []).extend(noms)
    if manquantes:
        raise ConfigurationManquante(manquantes)


def odoo_est_configure() -> bool:
    """L'instance a-t-elle un ERP ? (no-ERP servi, ADR-0022)."""
    try:
        odoo()
    except ConfigurationManquante:
        return False
    return True


def bot_est_configure() -> bool:
    """L'instance a-t-elle un bot Telegram ? (BOT__TOKEN présent)."""
    try:
        bot()
    except ConfigurationManquante:
        return False
    return True


def vider_cache() -> None:
    """Vide le cache de tous les accessors (isolation des tests)."""
    for accessor in _ACCESSORS:
        accessor.cache_clear()
