"""Vue read-time des affaires SGE ouvertes (#276).

`affaires_ouvertes` est une transformation pure (Fn(jalons, maintenant) -> affaires)
qui roule les jalons de `flux_affaires` jusqu'à une vue cockpit : une ligne par affaire
*non soldée*, avec son dernier état et son ancienneté. L'ancienneté dépend de
« maintenant » → calculée à la lecture, jamais matérialisée (cf. core/CONTEXT.md).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from electricore.core.pipelines.affaires import affaires_ouvertes

PARIS = ZoneInfo("Europe/Paris")


def _jalon(affaire_id, jalon_num, etat, date_heure, *, statut, prestation="CFN", pdl="99000000000017"):
    return {
        "affaire_id": affaire_id,
        "origine": "initiee",
        "prestation": prestation,
        "prestation_libelle": prestation,
        "statut": statut,
        "pdl": pdl,
        "segment": "C5",
        "jalon_num": jalon_num,
        "affaire_jalon_id": f"{affaire_id}#{jalon_num}",
        "jalon_date_heure": date_heure,
        "affaire_etat": etat,
        "affaire_etat_libelle": etat,
    }


def test_garde_les_non_soldees_avec_leur_dernier_etat():
    # Deux affaires : une en cours (COURS), une terminée (TERMN). La vue ne garde que
    # la non soldée, en une ligne, avec l'état de son *dernier* jalon (max jalon_num).
    jalons = pl.DataFrame(
        [
            _jalon("AFF1", 1, "DMTR", datetime(2024, 11, 20, 9, tzinfo=PARIS), statut="COURS"),
            _jalon("AFF1", 2, "INPL", datetime(2024, 11, 22, 9, tzinfo=PARIS), statut="COURS"),
            _jalon("AFF2", 1, "DMTR", datetime(2024, 11, 20, 9, tzinfo=PARIS), statut="TERMN"),
            _jalon("AFF2", 2, "CPRE", datetime(2024, 11, 23, 9, tzinfo=PARIS), statut="TERMN"),
        ]
    )

    vue = affaires_ouvertes(jalons, maintenant=datetime(2024, 11, 25, 9, tzinfo=PARIS))

    assert vue["affaire_id"].to_list() == ["AFF1"]
    assert vue["dernier_etat"].to_list() == ["INPL"]


def test_anciennete_comptee_depuis_le_premier_jalon():
    # L'ancienneté = maintenant − date du *premier* jalon (le dépôt), pas du dernier :
    # c'est l'âge de l'affaire, ce qui dit « bloquée depuis X jours ».
    jalons = pl.DataFrame(
        [
            _jalon("AFF1", 1, "DMTR", datetime(2024, 11, 10, 9, tzinfo=PARIS), statut="COURS"),
            _jalon("AFF1", 2, "INPL", datetime(2024, 11, 22, 9, tzinfo=PARIS), statut="COURS"),
        ]
    )

    vue = affaires_ouvertes(jalons, maintenant=datetime(2024, 11, 25, 9, tzinfo=PARIS))

    assert vue["anciennete_jours"].to_list() == [15]  # 25 - 10 nov


def test_statut_null_traite_comme_en_attente_enedis():
    # Enedis envoie un <statut> vide → null pour une affaire fraîchement initiée (jalon 0
    # DMTR, demande transmise mais pas encore rangée). C'est une affaire OUVERTE « en
    # attente Enedis » : elle doit apparaître au cockpit avec son dernier état, au même
    # titre que COURS. Seules TERMN/ANNUL sont soldées.
    jalons = pl.DataFrame(
        [
            _jalon("PEND1", 0, "DMTR", datetime(2024, 11, 20, 9, tzinfo=PARIS), statut=None),
            _jalon("DONE1", 1, "CPRE", datetime(2024, 11, 23, 9, tzinfo=PARIS), statut="TERMN"),
        ]
    )

    vue = affaires_ouvertes(jalons, maintenant=datetime(2024, 11, 25, 9, tzinfo=PARIS))

    assert vue["affaire_id"].to_list() == ["PEND1"]
    assert vue["dernier_etat"].to_list() == ["DMTR"]


def test_ame_exclu_mais_prestation_nulle_gardee():
    # AME (45 % du volume) = souscription de flux de données, pas une intervention de
    # périmètre → écarté du cockpit. Une affaire à prestation nulle (réclamation) est
    # gardée (le null ne doit pas être avalé par le filtre d'exclusion).
    jalons = pl.DataFrame(
        [
            _jalon("AME1", 1, "DMTR", datetime(2024, 11, 20, 9, tzinfo=PARIS), statut="COURS", prestation="AME"),
            _jalon("CFN1", 1, "DMTR", datetime(2024, 11, 20, 9, tzinfo=PARIS), statut="COURS", prestation="CFN"),
            _jalon("REC1", 1, "DMTR", datetime(2024, 11, 20, 9, tzinfo=PARIS), statut="COURS", prestation=None),
        ]
    )

    vue = affaires_ouvertes(jalons, maintenant=datetime(2024, 11, 25, 9, tzinfo=PARIS))

    assert sorted(vue["affaire_id"].to_list()) == ["CFN1", "REC1"]
