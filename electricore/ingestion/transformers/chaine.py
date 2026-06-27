"""Discipline de **chaîne** d'ingestion : « attraper → compter → continuer » (ADR-0037, étendu #445).

Co-localise les deux pièces de l'escalade per-flux, au-dessus des trois étages
`decrypt | unzip | parse` qu'elles gouvernent :

  - `StatsChaine` — le compteur mutable partagé par les trois étages d'un même flux ;
  - `etape_chaine` — le combinateur (décorateur) qui enferme en un seul *seam* la
    discipline répétée mot pour mot dans les trois étages : capture totale de
    l'itération du travail, comptage au yield, log paramétré, **non-propagation**.

`StatsChaine` est *chain-wide* (relu par le runner) mais vivait historiquement dans
`crypto.py` ; les étages aval l'importaient depuis crypto (dépendance descendante
artificielle). Elle est désormais **définie ici**, à côté de la discipline qui la mute.
"""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from functools import wraps

logger = logging.getLogger(__name__)


@dataclass
class StatsChaine:
    """Compteur de la **chaîne** de transformation d'un flux — escalade per-flux (ADR-0037, étendu).

    Un seul objet mutable est partagé par les trois étages de la chaîne
    (`decrypt | unzip | parse`) d'un même flux (resource). Chaque étage incrémente ses
    compteurs au fil des fichiers ; le runner agrège ensuite par flux et décide si
    l'absence totale de documents produits malgré des échecs (`flux_aveugle`) doit faire
    échouer le job. Discipline uniforme « attraper → compter → continuer » : un fichier
    fautif est compté, jamais une raison d'interrompre le run.
    """

    fichiers: int = 0  # zips entrants (entrée de decrypt)
    dechiffres: int = 0  # déchiffrements réussis (decrypt OK)
    extraits: int = 0  # fichiers internes extraits par unzip
    documents: int = 0  # documents produits (parse OK) — entrée du prédicat
    echecs_dechiffrement: int = 0
    echecs_extraction: int = 0
    echecs_linearisation: int = 0

    def echecs(self) -> int:
        """Total des échecs sur les trois étages de la chaîne."""
        return self.echecs_dechiffrement + self.echecs_extraction + self.echecs_linearisation

    def flux_aveugle(self) -> bool:
        """Flux ayant des fichiers mais produisant 0 document malgré ≥ 1 échec → étage sombre.

        Prédicat généralisé (ADR-0037 ext.) : « aveugle » = a produit zéro document de bout
        en bout *et* a compté au moins un échec — quel qu'en soit l'étage (clé manquante,
        zip corrompu en masse, documents tous malformés). Un échec **isolé** noyé dans des
        documents (`documents > 0`) est toléré. Un flux **vide par nature** (aucun document
        mais aucun échec — un zip sans fichier interne) n'est PAS aveugle : c'est l'`echecs()`
        explicite qui distingue le drop-par-erreur du vide légitime.
        """
        return self.documents == 0 and self.echecs() > 0


def etape_chaine(
    *,
    succes: str,
    echec: str,
    libelle: str,
    cle_item: str,
    capture: type[Exception] = Exception,
    niveau_log: int = logging.WARNING,
) -> Callable:
    """Décorateur paramétré : enferme la discipline « attraper → compter → continuer ».

    Chaque étage de la chaîne ne déclare plus que **son travail** — un générateur
    `(item, *config) -> Iterator[dict]` — plus le nom des **deux compteurs** `StatsChaine`
    à incrémenter (`succes`/`echec`). La discipline (capture de l'itération, comptage au
    yield, log paramétré, non-propagation) vit dans cet unique *seam*.

    Convention d'appel de l'enveloppe : `enveloppe(item, *config, stats)` — la factory
    `@dlt.transformer` injecte `stats` en **dernier positionnel** ; le travail ne le voit pas.

    Args:
        succes: Nom de l'attribut `StatsChaine` incrémenté à chaque document yieldé.
        echec: Nom de l'attribut `StatsChaine` incrémenté quand le travail lève.
        libelle: Mot du log (« déchiffrement », « extraction », « linéarisation »).
        cle_item: Clé de l'item dont la valeur identifie le fichier fautif dans le log.
        capture: Classe d'exception attrapée (resserrée par étage : `ValueError` pour
            decrypt). Une exception **hors** de cette classe propage.
        niveau_log: Niveau du log d'échec (`WARNING` par défaut, `ERROR` pour unzip).
    """

    def decorate(travail: Callable[..., Iterator[dict]]) -> Callable[..., Iterator[dict]]:
        @wraps(travail)
        def enveloppe(item, *config_et_stats):
            *config, stats = config_et_stats  # stats = dernier positionnel (injecté par la factory)
            try:
                for doc in travail(item, *config):  # le travail NE voit PAS stats
                    setattr(stats, succes, getattr(stats, succes) + 1)  # +1 AVANT le yield
                    yield doc
            except capture as e:  # noqa: BLE001 — compté, JAMAIS propagé (pin du spike : sinon dlt aborte l'extract)
                setattr(stats, echec, getattr(stats, echec) + 1)
                logger.log(niveau_log, "Échec %s %s : %s", libelle, item.get(cle_item, "?"), e)
                return

        return enveloppe

    return decorate
