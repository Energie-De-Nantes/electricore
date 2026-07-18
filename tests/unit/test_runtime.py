"""Tests du registre runtime (`electricore/config/runtime.py`, issue #141, ADR-0025).

Domaines pydantic-settings indépendants, accessors mis en cache, fail-fast
par point d'entrée via `valider()`. Les tests instancient les domaines avec
des valeurs d'init explicites quand c'est possible ; les comportements liés
à l'absence de variables passent par `monkeypatch` (delenv) avec le fichier
`.env` du dépôt neutralisé pour ne pas dépendre du poste.
"""

import pytest

from electricore.config import runtime


@pytest.fixture(autouse=True)
def _isolation_env(monkeypatch):
    """Neutralise le .env du dépôt et vide le cache des accessors."""
    monkeypatch.setattr(runtime, "FICHIER_ENV", None)
    runtime.vider_cache()
    yield
    runtime.vider_cache()


class TestDomaineDuckdb:
    def test_duckdb_path_definit_le_chemin(self, monkeypatch):
        monkeypatch.setenv("DUCKDB__PATH", "/data/flux.duckdb")
        assert str(runtime.duckdb().chemin) == "/data/flux.duckdb"

    def test_defaut_ancre_sur_le_depot_independant_du_cwd(self, monkeypatch, tmp_path):
        """Sans DUCKDB__PATH, la base de prod locale — absolue, jamais relative au CWD."""
        monkeypatch.delenv("DUCKDB__PATH", raising=False)
        chemin = runtime.duckdb().chemin
        monkeypatch.chdir(tmp_path)
        runtime.vider_cache()
        assert runtime.duckdb().chemin == chemin
        assert chemin.is_absolute()
        assert chemin.name == "flux_enedis_pipeline.duckdb"


class TestDomaineSftp:
    def test_url_lue_depuis_l_env(self, monkeypatch):
        monkeypatch.setenv("SFTP__URL", "sftp://user:pass@sftp.enedis.fr")
        assert runtime.sftp().url == "sftp://user:pass@sftp.enedis.fr"

    def test_url_manquante_leve_configuration_manquante(self, monkeypatch):
        monkeypatch.delenv("SFTP__URL", raising=False)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.sftp()
        assert "SFTP__URL" in str(exc.value)

    def test_configuration_manquante_est_une_value_error(self):
        """Compatibilité avec les `except ValueError` existants (façade odoo)."""
        assert issubclass(runtime.ConfigurationManquante, ValueError)

    @pytest.mark.parametrize(
        "url",
        [
            "sftp://user:pass@sftp.enedis.fr:22/exports",
            "file:///var/enedis/",
            "https://sftp.example.com",
            "http://localhost:8080",
        ],
    )
    def test_url_schema_supporte_accepte(self, monkeypatch, url):
        monkeypatch.setenv("SFTP__URL", url)
        assert runtime.sftp().url == url

    @pytest.mark.parametrize(
        "url",
        [
            "not-a-url",
            "ftp://sftp.example.com",
            "sftp.example.com",
        ],
    )
    def test_url_schema_invalide_rejete(self, monkeypatch, url):
        monkeypatch.setenv("SFTP__URL", url)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.sftp()
        assert "SFTP__URL" in str(exc.value)


HEX_A = "aa" * 16
HEX_B = "bb" * 16
HEX_C = "cc" * 16


class TestDomaineAes:
    """Trousseau N-clés d'ADR-0037, format env AES__TROUSSEAU__<label>__KEY/IV."""

    def test_trousseau_n_cles(self, monkeypatch):
        """AES__TROUSSEAU__<label>__KEY/IV peuple un trousseau ; chaine() rend les N entrées labellisées."""
        monkeypatch.setenv("AES__TROUSSEAU__aes256_2026__KEY", "ee" * 32)
        monkeypatch.setenv("AES__TROUSSEAU__aes256_2026__IV", HEX_B)
        monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__KEY", HEX_A)
        monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__IV", HEX_C)
        assert set(runtime.aes().chaine()) == {
            ("aes256_2026", bytes.fromhex("ee" * 32), bytes.fromhex(HEX_B)),
            ("aes128_2024", bytes.fromhex(HEX_A), bytes.fromhex(HEX_C)),
        }

    def test_cle_unique(self, monkeypatch):
        """Un trousseau d'une seule clé : chaine() rend cette unique entrée labellisée."""
        monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__KEY", HEX_A)
        monkeypatch.setenv("AES__TROUSSEAU__aes128_2024__IV", HEX_B)
        assert runtime.aes().chaine() == [("aes128_2024", bytes.fromhex(HEX_A), bytes.fromhex(HEX_B))]

    def test_trousseau_vide_leve_configuration_manquante(self):
        """Aucune clé AES__TROUSSEAU__* (.env neutralisé) → ConfigurationManquante nommant le format attendu."""
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.aes()
        assert "AES__TROUSSEAU__<label>__KEY" in str(exc.value)

    @pytest.mark.parametrize(
        "key,label",
        [
            ("pas-du-hex", "bad1"),
            ("aa" * 15, "bad2"),  # 30 chars, trop court
            ("zz" * 32, "bad3"),  # 64 non-hex
        ],
    )
    def test_cle_aes_invalide_rejetee_a_l_instanciation(self, monkeypatch, key, label):
        """Une KEY AES non-hex ou de longueur ≠ 32/64 est rejetée à l'instanciation du domaine aes."""
        monkeypatch.setenv(f"AES__TROUSSEAU__{label}__KEY", key)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.aes()
        assert f"AES__TROUSSEAU__{label.upper()}__KEY" in str(exc.value)

    def test_iv_aes_invalide_rejete_a_l_instanciation(self, monkeypatch):
        """Un IV non-hex est rejeté à l'instanciation, la variable IV est nommée."""
        monkeypatch.setenv("AES__TROUSSEAU__bad__KEY", HEX_A)
        monkeypatch.setenv("AES__TROUSSEAU__bad__IV", "pas-du-hex")
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.aes()
        assert "AES__TROUSSEAU__BAD__IV" in str(exc.value)


class TestDomaineOdoo:
    """Bloc unique ODOO__{URL,DB,USERNAME,PASSWORD} (#439, ADR-0046 §5).

    Plus de sélecteur test/prod : #190 clos completed, Odoo reste read-only (ADR-0012).
    """

    @pytest.fixture(autouse=True)
    def _env_odoo(self, monkeypatch):
        monkeypatch.setenv("ODOO__URL", "https://odoo.example")
        monkeypatch.setenv("ODOO__DB", "edn")
        monkeypatch.setenv("ODOO__USERNAME", "bot")
        monkeypatch.setenv("ODOO__PASSWORD", "secret")
        # Les anciens noms test/prod ne doivent plus rien piloter.
        for var in (
            "ODOO_ENV",
            "ODOO_TEST_URL",
            "ODOO_TEST_DB",
            "ODOO_TEST_USERNAME",
            "ODOO_TEST_PASSWORD",
            "ODOO_PROD_URL",
            "ODOO_PROD_DB",
            "ODOO_PROD_USERNAME",
            "ODOO_PROD_PASSWORD",
        ):
            monkeypatch.delenv(var, raising=False)

    def test_bloc_unique_sans_selecteur(self):
        """Le dict attendu par OdooReader : url, db, username, password."""
        assert runtime.odoo().model_dump() == {
            "url": "https://odoo.example",
            "db": "edn",
            "username": "bot",
            "password": "secret",
        }

    @pytest.mark.parametrize(
        "brut",
        [
            '"https://odoo.example"',  # guillemets doubles (sops exec-env les passe verbatim, #454)
            "'https://odoo.example'",  # guillemets simples
            "  https://odoo.example  ",  # espaces parasites
            '  "https://odoo.example"  ',  # espaces + guillemets
        ],
    )
    def test_url_normalisee_trim_et_dequote(self, monkeypatch, brut):
        """Une `ODOO__URL` entre guillemets / avec espaces est nettoyée au chargement (#454)."""
        monkeypatch.setenv("ODOO__URL", brut)
        assert runtime.odoo().url == "https://odoo.example"

    def test_db_entre_guillemets_dequote(self, monkeypatch):
        """`ODOO__DB` entre guillemets doubles (sops exec-env verbatim) est dé-quotée.

        Sans ça, le nom de base littéral `"edn"` atteint `authenticate()` et Postgres
        répond `database ""edn"" does not exist` (même classe que #454 pour l'URL).
        """
        monkeypatch.setenv("ODOO__DB", '"mainteniste-edn-odoo-edn-main-25347145"')
        assert runtime.odoo().db == "mainteniste-edn-odoo-edn-main-25347145"

    def test_username_entre_guillemets_dequote(self, monkeypatch):
        """`ODOO__USERNAME` quotée échoue à l'auth (login littéral `"bot"` introuvable)."""
        monkeypatch.setenv("ODOO__USERNAME", '"bot"')
        assert runtime.odoo().username == "bot"

    def test_password_entre_guillemets_dequote(self, monkeypatch):
        """`ODOO__PASSWORD` dé-quotée pour parité dev↔prod.

        En dev le `.env` passe par python-dotenv qui retire les guillemets appariés ;
        sans le même traitement en prod (`sops exec-env` verbatim), un secret écrit
        `ODOO__PASSWORD="secret"` authentifierait avec deux mots de passe différents
        selon l'environnement.
        """
        monkeypatch.setenv("ODOO__PASSWORD", '"secret"')
        assert runtime.odoo().password == "secret"

    def test_url_sans_schema_signalee_clairement(self, monkeypatch):
        """Un schéma absent ne tombe plus en 503 cryptique : message explicite nommant ODOO__URL."""
        monkeypatch.setenv("ODOO__URL", "odoo.example")
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.odoo()
        assert "ODOO__URL doit commencer par http:// ou https://" in str(exc.value)

    def test_no_erp_bloc_absent(self, monkeypatch):
        """Odoo absent (no-ERP, ADR-0022) : non configuré + manquantes listées en ODOO__*."""
        for champ in ("URL", "DB", "USERNAME", "PASSWORD"):
            monkeypatch.delenv(f"ODOO__{champ}", raising=False)
        assert runtime.odoo_est_configure() is False
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.odoo()
        assert "ODOO__URL" in str(exc.value)
        assert "ODOO__PASSWORD" in str(exc.value)

    def test_selecteur_supprime(self, monkeypatch):
        """#439 : le sélecteur ODOO_ENV et ses symboles disparaissent ; legacy ignoré."""
        assert not hasattr(runtime, "_SelecteurOdoo")
        assert not hasattr(runtime, "odoo_env_actif")
        # Des ODOO_TEST_*/ODOO_ENV résiduels ne doivent plus rien piloter.
        monkeypatch.setenv("ODOO_ENV", "prod")
        monkeypatch.setenv("ODOO_TEST_URL", "https://legacy.example")
        monkeypatch.setenv("ODOO_PROD_URL", "https://legacy-prod.example")
        assert runtime.odoo().url == "https://odoo.example"

    def test_facade_charger_config_odoo_sans_argument(self):
        """La façade `charger_config_odoo()` (notebooks) lit le bloc unique, sans env."""
        from electricore.config import charger_config_odoo

        assert charger_config_odoo() == {
            "url": "https://odoo.example",
            "db": "edn",
            "username": "bot",
            "password": "secret",
        }


class TestDomaineApi:
    def test_trousseau_etiquete_authentifie(self, monkeypatch):
        """ADR-0046 §4 : une clé API__TROUSSEAU__<consommateur>__KEY est acceptée."""
        monkeypatch.setenv("API__TROUSSEAU__librewatt__KEY", "k-librewatt-xxxxxxxxxxxxxxxxxxxx")
        api = runtime.api()
        assert api.cle_valide("k-librewatt-xxxxxxxxxxxxxxxxxxxx")
        assert "k-librewatt-xxxxxxxxxxxxxxxxxxxx" in api.cles_valides()

    def test_attribution_du_consommateur(self, monkeypatch):
        """ADR-0046 §4 : la clé entrante est attribuée à son consommateur (label)."""
        monkeypatch.setenv("API__TROUSSEAU__librewatt__KEY", "k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")
        monkeypatch.setenv("API__TROUSSEAU__odoo__KEY", "k-odoo-xxxxxxxxxxxxxxxxxxxxxxxxxxx")
        api = runtime.api()
        assert api.consommateur_pour("k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx") == "librewatt"
        assert api.consommateur_pour("k-odoo-xxxxxxxxxxxxxxxxxxxxxxxxxxx") == "odoo"
        assert api.consommateur_pour("inconnue") is None

    def test_revocation_ciblee_d_un_consommateur(self, monkeypatch):
        """ADR-0046 §4 : retirer la clé d'un consommateur ne touche pas les autres."""
        monkeypatch.setenv("API__TROUSSEAU__librewatt__KEY", "k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")
        monkeypatch.setenv("API__TROUSSEAU__odoo__KEY", "k-odoo-xxxxxxxxxxxxxxxxxxxxxxxxxxx")
        assert runtime.api().cle_valide("k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")
        assert runtime.api().cle_valide("k-odoo-xxxxxxxxxxxxxxxxxxxxxxxxxxx")
        monkeypatch.delenv("API__TROUSSEAU__odoo__KEY")
        runtime.vider_cache()
        api = runtime.api()
        assert api.cle_valide("k-librewatt-xxxxxxxxxxxxxxxxxxxxxxx")  # l'autre consommateur survit
        assert not api.cle_valide("k-odoo-xxxxxxxxxxxxxxxxxxxxxxxxxxx")  # le révoqué est rejeté

    def test_cle_bare_api_key_refusee(self, monkeypatch):
        """ADR-0046 §4 : le bare API_KEY/API_KEYS est retiré (trousseau-only)."""
        monkeypatch.setenv("API_KEY", "ancienne-cle-plate-bare-xxxxxxxxx")
        api = runtime.api()
        assert not api.cle_valide("ancienne-cle-plate-bare-xxxxxxxxx")
        assert api.cles_valides() == []

    def test_cles_valides_liste_les_cles_du_trousseau(self):
        api = runtime.Api(
            trousseau={
                "principal": {"key": "k-principale-xxxxxxxxxxxxxxxxxxx"},
                "second": {"key": "k-second-xxxxxxxxxxxxxxxxxxxxxxxxxxx"},
            }
        )
        assert api.cles_valides() == ["k-principale-xxxxxxxxxxxxxxxxxxx", "k-second-xxxxxxxxxxxxxxxxxxxxxxxxxxx"]

    def test_cle_valide_compare_digest(self):
        api = runtime.Api(trousseau={"c": {"key": "bonne-cle-xxxxxxxxxxxxxxxxxxxxxxxx"}})
        assert api.cle_valide("bonne-cle-xxxxxxxxxxxxxxxxxxxxxxxx")
        assert not api.cle_valide("mauvaise-cle")
        assert not api.cle_valide("")

    def test_cle_api_courte_rejetee(self, monkeypatch):
        """Une clé API < 32 caractères est rejetée par le domaine api."""
        monkeypatch.setenv("API__TROUSSEAU__test__KEY", "trop-courte-31-xxxx-yyyy-zzz")  # 28 chars
        with pytest.raises(runtime.ConfigurationManquante):
            runtime.api()

    def test_cle_api_32_chars_acceptee(self, monkeypatch):
        cle = "a" * 32
        monkeypatch.setenv("API__TROUSSEAU__test__KEY", cle)
        assert runtime.api().cle_valide(cle)

    def test_sans_cle_configuree_rien_ne_passe(self):
        api = runtime.Api()
        assert api.cles_valides() == []
        assert not api.cle_valide("nimporte")

    def test_version_par_defaut_suit_le_package(self):
        """API_VERSION absente → version du package electricore (pas de '0.0.0' en dur)."""
        import re

        assert re.match(r"\d+\.\d+", runtime.Api().version)

    def test_metadonnees_lues_depuis_api_prefixe(self, monkeypatch):
        """ADR-0046 : API__TITLE / API__VERSION / API__DESCRIPTION."""
        monkeypatch.setenv("API__TITLE", "Mon API")
        monkeypatch.setenv("API__VERSION", "9.9.9")
        monkeypatch.setenv("API__DESCRIPTION", "desc")
        api = runtime.api()
        assert (api.titre, api.version, api.description) == ("Mon API", "9.9.9", "desc")


class TestDomaineBot:
    def test_token_lu_depuis_bot_token(self, monkeypatch):
        """ADR-0046 : le préfixe suit le domaine runtime (bot()), pas le fournisseur."""
        monkeypatch.setenv("BOT__TOKEN", "123:abc")
        assert runtime.bot().token == "123:abc"

    def test_ancien_nom_telegram_ne_resout_plus(self, monkeypatch):
        """ADR-0046 : pas de double alias — l'ancien TELEGRAM_BOT_TOKEN est mort."""
        monkeypatch.delenv("BOT__TOKEN", raising=False)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
        with pytest.raises(runtime.ConfigurationManquante):
            runtime.bot()

    def test_token_manquant_leve_configuration_manquante(self, monkeypatch):
        monkeypatch.delenv("BOT__TOKEN", raising=False)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.bot()
        assert "BOT__TOKEN" in str(exc.value)

    def test_allowlist_et_notify_lus_depuis_bot(self, monkeypatch):
        """ADR-0046 : BOT__ALLOWED_USERS / BOT__NOTIFY_CHAT_ID (ex TELEGRAM_*)."""
        monkeypatch.setenv("BOT__TOKEN", "t")
        monkeypatch.setenv("BOT__ALLOWED_USERS", "1, 2")
        monkeypatch.setenv("BOT__NOTIFY_CHAT_ID", "-100")
        bot = runtime.bot()
        assert bot.utilisateurs_autorises() == {1, 2}
        assert bot.notify_chat_id == "-100"

    def test_allowlist_parse_en_ids_entiers(self):
        bot = runtime.Bot(token="t", allowed_users="123, 456 ,abc,")
        assert bot.utilisateurs_autorises() == {123, 456}

    def test_allowlist_vide_par_defaut(self):
        bot = runtime.Bot(token="t")
        assert bot.utilisateurs_autorises() == set()
        assert bot.notify_chat_id == ""


class TestDomaineRelais:
    """Relais de flux Enedis déchiffrés vers SFTP partenaire (#637) — domaine dédié."""

    def test_urls_lues_depuis_l_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("RELAIS__SOURCE_URL", "file:///var/enedis/")
        monkeypatch.setenv("RELAIS__PARTNER_URL", "sftp://user:pass@partenaire.example/in")
        r = runtime.relais()
        assert r.source_url == "file:///var/enedis/"
        assert r.partner_url == "sftp://user:pass@partenaire.example/in"

    def test_schema_invalide_rejete(self, monkeypatch):
        monkeypatch.setenv("RELAIS__SOURCE_URL", "not-a-url")
        monkeypatch.setenv("RELAIS__PARTNER_URL", "file:///tmp/out")
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.relais()
        assert "RELAIS__SOURCE_URL" in str(exc.value)

    def test_destination_db_par_defaut_absolue(self, monkeypatch):
        monkeypatch.setenv("RELAIS__SOURCE_URL", "file:///a")
        monkeypatch.setenv("RELAIS__PARTNER_URL", "file:///b")
        assert runtime.relais().destination_db.is_absolute()
        assert runtime.relais().destination_db.name == "relais.duckdb"

    def test_flux_filtres_vide_par_defaut(self):
        r = runtime.Relais(source_url="file:///a", partner_url="file:///b")
        assert r.flux_filtres() is None
        assert r.depuis == "2026-06-01"

    def test_flux_filtres_parse_csv_majuscule(self):
        r = runtime.Relais(source_url="file:///a", partner_url="file:///b", flux="c15, r151 ,")
        assert r.flux_filtres() == {"C15", "R151"}


class TestValider:
    """Fail-fast par point d'entrée : une seule erreur listant tout ce qui manque."""

    def test_agrege_les_manquantes_de_tous_les_domaines(self, monkeypatch):
        monkeypatch.delenv("SFTP__URL", raising=False)
        monkeypatch.delenv("BOT__TOKEN", raising=False)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.valider(runtime.sftp, runtime.bot)
        assert "SFTP__URL" in str(exc.value)
        assert "BOT__TOKEN" in str(exc.value)
        assert set(exc.value.manquantes) == {"sftp", "bot"}

    def test_silencieux_quand_tout_est_la(self, monkeypatch):
        monkeypatch.setenv("SFTP__URL", "sftp://x")
        monkeypatch.setenv("BOT__TOKEN", "t")
        runtime.valider(runtime.sftp, runtime.bot)  # ne lève pas


class TestAccessors:
    def test_mis_en_cache(self, monkeypatch):
        monkeypatch.setenv("SFTP__URL", "sftp://x")
        assert runtime.sftp() is runtime.sftp()

    def test_odoo_est_configure(self, monkeypatch):
        for var in ("ODOO__URL", "ODOO__DB", "ODOO__USERNAME", "ODOO__PASSWORD"):
            monkeypatch.delenv(var, raising=False)
        assert not runtime.odoo_est_configure()
        monkeypatch.setenv("ODOO__URL", "https://x")
        monkeypatch.setenv("ODOO__DB", "d")
        monkeypatch.setenv("ODOO__USERNAME", "u")
        monkeypatch.setenv("ODOO__PASSWORD", "p")
        runtime.vider_cache()
        assert runtime.odoo_est_configure()


# ── Parité de frontière (ADR-0046) ──────────────────────────────────────────
# Les exemples de déploiement (deploy/providers/example/) déclarent EXACTEMENT
# les noms que le runtime lit. Un respell d'un côté non miroité de l'autre casse
# ces tests — c'est ce qui rend la convention <DOMAINE>__<CHAMP> *appliquée*, pas
# juste aspirationnelle.

import pathlib  # noqa: E402

_RACINE = pathlib.Path(__file__).resolve().parents[2]
_EXEMPLES = (
    _RACINE / "deploy/providers/example/config.env.example",
    _RACINE / "deploy/providers/example/secrets.env.example",
)
# Noms retirés par ADR-0046 : #436 (TELEGRAM_/DUCKDB_PATH/API_*), #438 (API_KEY)
# et #439 (sélecteur Odoo ODOO_ENV + blocs ODOO_TEST_/ODOO_PROD_ → bloc unique ODOO__*).
_NOMS_RETIRES = (
    "TELEGRAM_",
    "DUCKDB_PATH",
    "API_TITLE",
    "API_VERSION",
    "API_DESCRIPTION",
    "API_KEY",
    "ODOO_ENV",
    "ODOO_TEST_",
    "ODOO_PROD_",
)


def _charger_dotenv(path: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for ligne in path.read_text().splitlines():
        ligne = ligne.strip()
        if not ligne or ligne.startswith("#") or "=" not in ligne:
            continue
        cle, _, val = ligne.partition("=")
        env[cle.strip()] = val.strip()
    return env


class TestPariteFrontiereDeploiement:
    def test_les_exemples_satisfont_les_domaines_a_credentials(self, monkeypatch):
        """Peuplé depuis les seuls exemples, le runtime valide ses domaines à
        credentials requis (odoo inclus depuis #439). Un nom mal respellé → manquante."""
        for path in _EXEMPLES:
            for cle, val in _charger_dotenv(path).items():
                monkeypatch.setenv(cle, val)
        runtime.valider(runtime.sftp, runtime.aes, runtime.bot, runtime.odoo)

    def test_aucun_ancien_nom_dans_les_exemples(self):
        for path in _EXEMPLES:
            texte = path.read_text()
            for nom in _NOMS_RETIRES:
                assert nom not in texte, f"nom retiré {nom!r} encore présent dans {path.name}"

    def test_l_exemple_declare_au_moins_un_consommateur_api(self, monkeypatch):
        """ADR-0046 §4 : secrets.env.example fournit ≥ 1 clé API__TROUSSEAU__<c>__KEY."""
        for path in _EXEMPLES:
            for cle, val in _charger_dotenv(path).items():
                monkeypatch.setenv(cle, val)
        assert runtime.api().cles_valides()  # le trousseau API n'est pas vide
