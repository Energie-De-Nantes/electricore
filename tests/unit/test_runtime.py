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
        monkeypatch.setenv("DUCKDB_PATH", "/data/flux.duckdb")
        assert str(runtime.duckdb().chemin) == "/data/flux.duckdb"

    def test_defaut_ancre_sur_le_depot_independant_du_cwd(self, monkeypatch, tmp_path):
        """Sans DUCKDB_PATH, la base de prod locale — absolue, jamais relative au CWD."""
        monkeypatch.delenv("DUCKDB_PATH", raising=False)
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


HEX_A = "aa" * 16
HEX_B = "bb" * 16
HEX_C = "cc" * 16
HEX_D = "dd" * 16


class TestDomaineAes:
    """Cascade de rotation d'ADR-0008, format env AES__CURRENT__* / AES__PREVIOUS__*."""

    def test_current_seule(self, monkeypatch):
        monkeypatch.setenv("AES__CURRENT__KEY", HEX_A)
        monkeypatch.setenv("AES__CURRENT__IV", HEX_B)
        for var in ("AES__PREVIOUS__KEY", "AES__PREVIOUS__IV", "AES__KEY", "AES__IV"):
            monkeypatch.delenv(var, raising=False)
        assert runtime.aes().chaine() == [("current", bytes.fromhex(HEX_A), bytes.fromhex(HEX_B))]

    def test_rotation_current_puis_previous(self, monkeypatch):
        monkeypatch.setenv("AES__CURRENT__KEY", HEX_A)
        monkeypatch.setenv("AES__CURRENT__IV", HEX_B)
        monkeypatch.setenv("AES__PREVIOUS__KEY", HEX_C)
        monkeypatch.setenv("AES__PREVIOUS__IV", HEX_D)
        assert [nom for nom, _, _ in runtime.aes().chaine()] == ["current", "previous"]

    def test_format_legacy_v1(self, monkeypatch):
        for var in ("AES__CURRENT__KEY", "AES__CURRENT__IV", "AES__PREVIOUS__KEY", "AES__PREVIOUS__IV"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("AES__KEY", HEX_A)
        monkeypatch.setenv("AES__IV", HEX_B)
        assert runtime.aes().chaine() == [("legacy", bytes.fromhex(HEX_A), bytes.fromhex(HEX_B))]

    def test_aucune_cle_leve_configuration_manquante(self, monkeypatch):
        for var in (
            "AES__CURRENT__KEY",
            "AES__CURRENT__IV",
            "AES__PREVIOUS__KEY",
            "AES__PREVIOUS__IV",
            "AES__KEY",
            "AES__IV",
        ):
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.aes()
        assert "AES__CURRENT__KEY" in str(exc.value)

    def test_hex_invalide_signale(self):
        aes = runtime.Aes(current=runtime.PaireCles(key="pas-du-hex", iv=HEX_B))
        with pytest.raises(ValueError, match="AES__CURRENT"):
            aes.chaine()


class TestDomaineOdoo:
    """Sélecteur ODOO_ENV (test/prod), préfixe ODOO_{ENV}_ injecté à l'init.

    Mécanique gelée dans l'attente de #190 — mêmes vars, mêmes valeurs.
    """

    @pytest.fixture(autouse=True)
    def _env_odoo(self, monkeypatch):
        monkeypatch.setenv("ODOO_TEST_URL", "https://test.odoo.example")
        monkeypatch.setenv("ODOO_TEST_DB", "edn-test")
        monkeypatch.setenv("ODOO_TEST_USERNAME", "bot")
        monkeypatch.setenv("ODOO_TEST_PASSWORD", "secret")
        for var in ("ODOO_PROD_URL", "ODOO_PROD_DB", "ODOO_PROD_USERNAME", "ODOO_PROD_PASSWORD"):
            monkeypatch.delenv(var, raising=False)

    def test_selecteur_par_defaut_test(self, monkeypatch):
        monkeypatch.delenv("ODOO_ENV", raising=False)
        config = runtime.odoo()
        assert config.url == "https://test.odoo.example"
        assert config.db == "edn-test"

    def test_env_explicite_prime_sur_le_selecteur(self, monkeypatch):
        monkeypatch.setenv("ODOO_ENV", "prod")
        assert runtime.odoo("test").db == "edn-test"

    def test_prod_incomplete_liste_les_vraies_vars(self, monkeypatch):
        """Le footgun ADR-0025 : ODOO_ENV=prod sans vars prod doit échouer fort."""
        monkeypatch.setenv("ODOO_ENV", "prod")
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.odoo()
        assert "ODOO_PROD_URL" in str(exc.value)
        assert "ODOO_PROD_PASSWORD" in str(exc.value)

    def test_model_dump_compatible_odoo_reader(self):
        """Le dict attendu par OdooReader : url, db, username, password."""
        assert runtime.odoo("test").model_dump() == {
            "url": "https://test.odoo.example",
            "db": "edn-test",
            "username": "bot",
            "password": "secret",
        }


class TestDomaineApi:
    def test_cles_valides_combine_api_key_et_api_keys(self):
        api = runtime.Api(cle="k-principale", cles="k-2, k-3 ,")
        assert api.cles_valides() == ["k-principale", "k-2", "k-3"]

    def test_cle_valide_compare_digest(self):
        api = runtime.Api(cle="bonne-cle")
        assert api.cle_valide("bonne-cle")
        assert not api.cle_valide("mauvaise-cle")
        assert not api.cle_valide("")

    def test_sans_cle_configuree_rien_ne_passe(self):
        api = runtime.Api()
        assert api.cles_valides() == []
        assert not api.cle_valide("nimporte")

    def test_version_par_defaut_suit_le_package(self):
        """API_VERSION absente → version du package electricore (pas de '0.0.0' en dur)."""
        import re

        assert re.match(r"\d+\.\d+", runtime.Api().version)


class TestDomaineBot:
    def test_token_manquant_leve_configuration_manquante(self, monkeypatch):
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.bot()
        assert "TELEGRAM_BOT_TOKEN" in str(exc.value)

    def test_allowlist_parse_en_ids_entiers(self):
        bot = runtime.Bot(token="t", allowed_users="123, 456 ,abc,")
        assert bot.utilisateurs_autorises() == {123, 456}

    def test_allowlist_vide_par_defaut(self):
        bot = runtime.Bot(token="t")
        assert bot.utilisateurs_autorises() == set()
        assert bot.notify_chat_id == ""


class TestValider:
    """Fail-fast par point d'entrée : une seule erreur listant tout ce qui manque."""

    def test_agrege_les_manquantes_de_tous_les_domaines(self, monkeypatch):
        monkeypatch.delenv("SFTP__URL", raising=False)
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        with pytest.raises(runtime.ConfigurationManquante) as exc:
            runtime.valider(runtime.sftp, runtime.bot)
        assert "SFTP__URL" in str(exc.value)
        assert "TELEGRAM_BOT_TOKEN" in str(exc.value)
        assert set(exc.value.manquantes) == {"sftp", "bot"}

    def test_silencieux_quand_tout_est_la(self, monkeypatch):
        monkeypatch.setenv("SFTP__URL", "sftp://x")
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "t")
        runtime.valider(runtime.sftp, runtime.bot)  # ne lève pas


class TestAccessors:
    def test_mis_en_cache(self, monkeypatch):
        monkeypatch.setenv("SFTP__URL", "sftp://x")
        assert runtime.sftp() is runtime.sftp()

    def test_odoo_est_configure(self, monkeypatch):
        for var in ("ODOO_TEST_URL", "ODOO_TEST_DB", "ODOO_TEST_USERNAME", "ODOO_TEST_PASSWORD"):
            monkeypatch.delenv(var, raising=False)
        monkeypatch.delenv("ODOO_ENV", raising=False)
        assert not runtime.odoo_est_configure()
        monkeypatch.setenv("ODOO_TEST_URL", "https://x")
        monkeypatch.setenv("ODOO_TEST_DB", "d")
        monkeypatch.setenv("ODOO_TEST_USERNAME", "u")
        monkeypatch.setenv("ODOO_TEST_PASSWORD", "p")
        runtime.vider_cache()
        assert runtime.odoo_est_configure()
