"""Tests pour `OdooWriter.update()` : rapport d'injection par-record (#571).

Avant #571, la boucle d'écriture s'arrêtait à la première exception XML-RPC :
Odoo restait à moitié injecté. Ces tests couvrent le nouveau comportement :
un échec par record ne casse pas la boucle, et chaque catégorie (réussi,
échoué, sans id) est rapportée — plus seulement loguée.
"""

from unittest.mock import MagicMock

from electricore.integrations.odoo.writers import OdooWriter

_CONFIG = {"url": "https://fake.odoo", "db": "fake", "username": "u", "password": "p"}


def _writer(execute_side_effect):
    writer = OdooWriter(config=_CONFIG, sim=False)
    # Contourne la connexion XML-RPC réelle : on mocke `execute` directement,
    # comme le ferait un appelant qui teste `update()` en isolation.
    writer.execute = MagicMock(side_effect=execute_side_effect)
    return writer


def test_update_continue_apres_echec_xmlrpc_sur_un_record():
    """Un échec XML-RPC sur le record 2 n'interrompt pas la boucle (#571)."""

    def _execute(model, method, args=None, kwargs=None):
        record_id = args[0][0]
        if record_id == 2:
            raise Exception("XML-RPC fault: access denied")
        return [True]

    writer = _writer(_execute)
    rapport = writer.update(
        "sale.order",
        [
            {"id": 1, "x_field": "a"},
            {"id": 2, "x_field": "b"},
            {"id": 3, "x_field": "c"},
        ],
    )

    # Les records 1 et 3 sont écrits malgré l'échec du 2 (plus d'arrêt net).
    assert writer.execute.call_count == 3
    assert rapport.ids_reussis == [1, 3]
    assert len(rapport.echecs) == 1
    assert rapport.echecs[0].id == 2
    assert "access denied" in rapport.echecs[0].raison
    assert rapport.sans_id == []


def test_update_liste_les_records_sans_id_dans_le_rapport():
    """Un record sans 'id' apparaît dans le rapport, plus seulement dans les logs (#571)."""
    writer = _writer(lambda *a, **k: [True])

    rapport = writer.update(
        "sale.order",
        [
            {"x_field": "sans id"},
            {"id": 5, "x_field": "ok"},
        ],
    )

    assert rapport.ids_reussis == [5]
    assert rapport.echecs == []
    assert len(rapport.sans_id) == 1
    assert rapport.sans_id[0]["x_field"] == "sans id"


def test_update_retour_ignorable_retrocompatible():
    """Un appelant qui ignore le retour (`injection_rsc.py`) continue de fonctionner (#571)."""
    writer = _writer(lambda *a, **k: [True])

    # Pas de capture du retour : simule l'appel existant `_writer.update(model, records)`.
    writer.update("sale.order", [{"id": 1}])


def test_update_mode_simulation_n_appelle_jamais_execute():
    """Mode simulation : aucun XML-RPC, mais le rapport liste quand même les ids visés."""
    writer = OdooWriter(config=_CONFIG, sim=True)
    writer.execute = MagicMock()

    rapport = writer.update("sale.order", [{"id": 1}, {"id": 2}])

    writer.execute.assert_not_called()
    assert rapport.ids_reussis == [1, 2]
    assert rapport.echecs == []
