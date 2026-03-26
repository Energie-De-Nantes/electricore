"""
Tests d'intégration OdooWriter — instance de test Odoo 18+.

Prérequis :
    - Instance Odoo de test accessible
    - Config dans .dlt/secrets.toml (section [odoo]) ou variable d'env ODOO_URL, etc.
    - Droits de création/modification/suppression sur res.partner

Lancer uniquement ces tests :
    uv run --group test pytest tests/integration/test_odoo_writer.py -v

Skippés automatiquement si aucune config Odoo n'est disponible.
"""

import pytest
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-reattr]

from electricore.core.writers import OdooWriter


# =============================================================================
# FIXTURES
# =============================================================================


def _load_odoo_config(section: str = 'odoo_test') -> dict | None:
    """
    Charge une config Odoo depuis secrets.toml.

    Args:
        section: Section TOML à lire. Par défaut 'odoo_test' (instance de test).
                 Passer 'odoo_prod' pour la production — choix explicite requis.

    Returns:
        Dict de config ou None si la section est absente.
    """
    secrets_paths = [
        Path.cwd() / '.dlt' / 'secrets.toml',
        Path.cwd() / 'electricore' / 'etl' / '.dlt' / 'secrets.toml',
    ]
    for path in secrets_paths:
        if path.exists():
            with open(path, 'rb') as f:
                data = tomllib.load(f)
            if section in data:
                return data[section]
    return None


@pytest.fixture(scope='session')
def odoo_config():
    """Config Odoo de test — skip si absente."""
    config = _load_odoo_config()
    if config is None:
        pytest.skip('Section [odoo_test] absente de secrets.toml — ajoutez-la pour activer ces tests')
    return config


@pytest.fixture(scope='session')
def odoo_writer(odoo_config):
    """OdooWriter connecté — réutilisé pour toute la session."""
    with OdooWriter(odoo_config) as writer:
        yield writer


@pytest.fixture(scope='session')
def odoo_writer_sim(odoo_config):
    """OdooWriter en mode simulation — ne fait aucune écriture réelle."""
    with OdooWriter(odoo_config, sim=True) as writer:
        yield writer


@pytest.fixture
def test_partner_cleanup(odoo_writer):
    """
    Fixture de cleanup : supprime après chaque test les partenaires créés.

    Usage : injecter dans le test et ajouter les IDs créés dans la liste.
    """
    created_ids: list[int] = []
    yield created_ids
    if created_ids:
        odoo_writer.execute('res.partner', 'unlink', [created_ids])


# =============================================================================
# T1 — Connexion et cycle de vie
# =============================================================================


class TestT1Connexion:
    """T1 — Connexion et cycle de vie."""

    def test_t1_1_non_connecte_avant_connect(self, odoo_config):
        """T1.1 — is_connected == False avant connect()."""
        writer = OdooWriter(odoo_config)
        assert not writer.is_connected

    def test_t1_2_context_manager(self, odoo_config):
        """T1.2 — Connexion active dans le bloc, fermée après."""
        with OdooWriter(odoo_config) as w:
            assert w.is_connected
        assert not w.is_connected

    def test_t1_3_mauvais_password(self, odoo_config):
        """T1.3 — Exception si mauvais mot de passe."""
        bad_config = {**odoo_config, 'password': 'mauvais_mot_de_passe_xxxxxxx'}
        with pytest.raises(Exception, match='Authentication failed'):
            OdooWriter(bad_config).connect()

    def test_t1_4_methode_non_autorisee(self, odoo_writer):
        """T1.4 — ValueError si méthode hors whitelist."""
        with pytest.raises(ValueError):
            odoo_writer.execute('res.partner', 'sudo_delete_all')


# =============================================================================
# T2 — Mode simulation
# =============================================================================


class TestT2Simulation:
    """T2 — Mode simulation."""

    def test_t2_1_create_sim_retourne_vide(self, odoo_writer_sim):
        """T2.1 — create() en sim retourne [] sans créer."""
        result = odoo_writer_sim.create('res.partner', [{'name': 'TEST SIM WRITER'}])
        assert result == []

    def test_t2_1_create_sim_pas_de_creation(self, odoo_writer, odoo_writer_sim):
        """T2.1 (vérif) — Aucun partenaire n'est créé en mode sim."""
        name = 'TEST SIM WRITER __CONTROLE__'
        odoo_writer_sim.create('res.partner', [{'name': name}])
        df = odoo_writer.search_read('res.partner', [['name', '=', name]], fields=['name'])
        assert len(df) == 0

    def test_t2_2_update_sim_pas_de_modification(self, odoo_writer, odoo_writer_sim, test_partner_cleanup):
        """T2.2 — update() en sim ne modifie pas les données."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST UPDATE SIM AVANT'}])
        test_partner_cleanup.extend(ids)

        odoo_writer_sim.update('res.partner', [{'id': ids[0], 'name': 'TEST UPDATE SIM APRES'}])

        df = odoo_writer.read('res.partner', ids, fields=['name'])
        assert df['name'][0] == 'TEST UPDATE SIM AVANT'


# =============================================================================
# T3 — Création (create)
# =============================================================================


class TestT3Create:
    """T3 — Création de records."""

    def test_t3_1_creer_un_enregistrement(self, odoo_writer, test_partner_cleanup):
        """T3.1 — Créer 1 enregistrement retourne [id]."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T3.1'}])
        test_partner_cleanup.extend(ids)

        assert isinstance(ids, list)
        assert len(ids) == 1
        assert isinstance(ids[0], int)

    def test_t3_2_creer_plusieurs_enregistrements(self, odoo_writer, test_partner_cleanup):
        """T3.2 — Créer N enregistrements retourne N IDs."""
        records = [{'name': f'TEST WRITER T3.2 #{i}'} for i in range(3)]
        ids = odoo_writer.create('res.partner', records)
        test_partner_cleanup.extend(ids)

        assert len(ids) == 3
        assert all(isinstance(i, int) for i in ids)

    def test_t3_3_creer_avec_champ_none(self, odoo_writer, test_partner_cleanup):
        """T3.3 — Champ None filtré, pas d'erreur XML-RPC."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T3.3', 'comment': None}])
        test_partner_cleanup.extend(ids)
        assert len(ids) == 1

    def test_t3_4_creer_avec_liste_vide(self, odoo_writer, test_partner_cleanup):
        """T3.4 — Champ [] vide filtré, pas d'erreur. Valide le fix #1."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T3.4', 'child_ids': []}])
        test_partner_cleanup.extend(ids)
        assert len(ids) == 1

    def test_t3_5_creer_liste_vide_records(self, odoo_writer):
        """T3.5 — records=[] retourne [] sans erreur."""
        result = odoo_writer.create('res.partner', [])
        assert result == []

    def test_t3_6_modele_inexistant(self, odoo_writer):
        """T3.6 — Exception XML-RPC si modèle inexistant."""
        with pytest.raises(Exception):
            odoo_writer.create('modele.inexistant.xyz', [{'name': 'test'}])

    def test_t3_7_round_trip_creation(self, odoo_writer, test_partner_cleanup):
        """T3.7 — Créer puis relire : champs cohérents."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T3.7 ROUND-TRIP'}])
        test_partner_cleanup.extend(ids)

        df = odoo_writer.read('res.partner', ids, fields=['name'])
        assert df['name'][0] == 'TEST WRITER T3.7 ROUND-TRIP'


# =============================================================================
# T4 — Mise à jour (update)
# =============================================================================


class TestT4Update:
    """T4 — Mise à jour de records."""

    def test_t4_1_mettre_a_jour_un_champ(self, odoo_writer, test_partner_cleanup):
        """T4.1 — Mise à jour d'un champ vérifiée par read()."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T4.1 AVANT'}])
        test_partner_cleanup.extend(ids)

        odoo_writer.update('res.partner', [{'id': ids[0], 'name': 'TEST WRITER T4.1 APRES'}])

        df = odoo_writer.read('res.partner', ids, fields=['name'])
        assert df['name'][0] == 'TEST WRITER T4.1 APRES'

    def test_t4_2_mettre_a_jour_plusieurs_records(self, odoo_writer, test_partner_cleanup):
        """T4.2 — Mise à jour de N enregistrements."""
        records_init = [{'name': f'TEST WRITER T4.2 AVANT #{i}'} for i in range(3)]
        ids = odoo_writer.create('res.partner', records_init)
        test_partner_cleanup.extend(ids)

        updates = [{'id': id_, 'name': f'TEST WRITER T4.2 APRES #{i}'} for i, id_ in enumerate(ids)]
        odoo_writer.update('res.partner', updates)

        df = odoo_writer.read('res.partner', ids, fields=['name'])
        for i, id_ in enumerate(ids):
            row = df.filter(df['res_partner_id'] == id_)
            assert row['name'][0] == f'TEST WRITER T4.2 APRES #{i}'

    def test_t4_3_sans_champ_id_skippage(self, odoo_writer):
        """T4.3 — Enregistrement sans 'id' : warning loggé, pas d'exception."""
        odoo_writer.update('res.partner', [{'name': 'sans ID, doit être ignoré'}])

    def test_t4_4_id_invalide(self, odoo_writer):
        """T4.4 — id inexistant : exception XML-RPC propagée."""
        with pytest.raises(Exception):
            odoo_writer.update('res.partner', [{'id': 999999999, 'name': 'test'}])

    def test_t4_5_update_avec_liste_vide(self, odoo_writer, test_partner_cleanup):
        """T4.5 — Champ [] vide dans update filtré, pas d'erreur. Valide le fix #1."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T4.5'}])
        test_partner_cleanup.extend(ids)

        odoo_writer.update('res.partner', [{'id': ids[0], 'child_ids': []}])

    def test_t4_6_clean_data_vide(self, odoo_writer, test_partner_cleanup):
        """T4.6 — clean_data vide après nettoyage : write({}) accepté par Odoo."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T4.6'}])
        test_partner_cleanup.extend(ids)

        # Tous les champs à None → clean_data = {}
        odoo_writer.update('res.partner', [{'id': ids[0], 'comment': None, 'website': None}])


# =============================================================================
# T5 — Sémantique None vs False
# =============================================================================


class TestT5NoneFalse:
    """T5 — Comportement None (ne pas toucher) vs False (effacer)."""

    def test_t5_1_none_ne_modifie_pas(self, odoo_writer, test_partner_cleanup):
        """T5.1 — None filtré : champ existant non modifié."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T5.1', 'website': 'https://example.com'}])
        test_partner_cleanup.extend(ids)

        odoo_writer.update('res.partner', [{'id': ids[0], 'website': None}])

        df = odoo_writer.read('res.partner', ids, fields=['website'])
        assert df['website'][0] == 'https://example.com'

    def test_t5_2_false_efface_le_champ(self, odoo_writer, test_partner_cleanup):
        """T5.2 — False envoyé à Odoo : champ effacé."""
        ids = odoo_writer.create('res.partner', [{'name': 'TEST WRITER T5.2', 'website': 'https://example.com'}])
        test_partner_cleanup.extend(ids)

        odoo_writer.update('res.partner', [{'id': ids[0], 'website': False}])

        df = odoo_writer.read('res.partner', ids, fields=['website'])
        assert df['website'][0] is None  # OdooReader normalise False → None

    def test_t5_3_round_trip_seul_le_champ_modifie_change(self, odoo_writer, test_partner_cleanup):
        """T5.3 — Round-trip complet : seul le champ modifié change."""
        ids = odoo_writer.create('res.partner', [{
            'name': 'TEST WRITER T5.3 AVANT',
            'website': 'https://avant.example.com',
        }])
        test_partner_cleanup.extend(ids)

        # Lire les données
        df_avant = odoo_writer.read('res.partner', ids, fields=['name', 'website'])
        website_avant = df_avant['website'][0]

        # Modifier seulement le nom
        odoo_writer.update('res.partner', [{'id': ids[0], 'name': 'TEST WRITER T5.3 APRES'}])

        df_apres = odoo_writer.read('res.partner', ids, fields=['name', 'website'])
        assert df_apres['name'][0] == 'TEST WRITER T5.3 APRES'
        assert df_apres['website'][0] == website_avant  # website inchangé
