"""Suite dédiée du combinateur `etape_chaine` (#479).

`etape_chaine` enferme en un seul *seam* la discipline « attraper → compter → continuer »
(ADR-0037 étendu, #445) jusqu'ici triplée mot pour mot dans `decrypt | unzip | parse` :

  - chaque **document yieldé** par le travail incrémente le compteur de succès (+1 AVANT
    le yield) — unifie le 1-yield de decrypt/parse et le N-yields d'unzip ;
  - le travail qui **lève** (dans la classe `capture=`) compte UN échec, log au niveau
    paramétré, puis se **termine** — JAMAIS de propagation (le pin du spike : une exception
    non rattrapée d'un transformer dlt aborte tout l'`extract`) ;
  - une exception **hors** de la classe capturée **propage** (capture resserrée par étage).

Travail-jouet ci-dessous : on n'a besoin d'aucun étage réel pour épingler la discipline.
La factory `@dlt.transformer` injecte `stats` en **dernier positionnel** ; le travail décoré
ne voit que `(item, *config)`.
"""

import logging

import pytest

from electricore.ingestion.transformers.chaine import StatsChaine, etape_chaine


def test_compte_un_succes_par_document_yielde():
    """Tracer bullet : chaque document yieldé par le travail vaut +1 au compteur de succès."""
    stats = StatsChaine()

    @etape_chaine(succes="documents", echec="echecs_linearisation", libelle="jouet", cle_item="nom")
    def travail(item, n):
        for i in range(n):
            yield {"i": i}

    produits = list(travail({"nom": "x"}, 3, stats))

    assert len(produits) == 3
    assert stats.documents == 3 and stats.echecs_linearisation == 0


def test_zero_document_sans_lever_ne_compte_ni_succes_ni_echec():
    """Travail qui n'émet rien et ne lève pas → 0 succès, 0 échec (vide par nature).

    Le combinateur ne traite JAMAIS « 0 document yieldé » comme un échec : seul le travail
    qui **lève** signale un échec."""
    stats = StatsChaine()

    @etape_chaine(succes="extraits", echec="echecs_extraction", libelle="jouet", cle_item="nom")
    def travail(item):
        return
        yield  # générateur vide

    produits = list(travail({"nom": "x"}, stats))

    assert produits == []
    assert stats.extraits == 0 and stats.echecs_extraction == 0


def test_lever_apres_k_yields_compte_k_succes_un_echec_sans_propager(caplog):
    """Pin du spike : un travail qui lève après k documents compte k succès + 1 échec,
    log au niveau paramétré (avec libellé + identité du fichier), et **ne propage JAMAIS**.

    C'est la garantie anti-`PipelineStepFailed` : une exception non rattrapée d'un transformer
    dlt aborterait tout l'`extract`. Le générateur décoré se termine simplement."""
    stats = StatsChaine()

    @etape_chaine(
        succes="extraits",
        echec="echecs_extraction",
        libelle="extraction",
        cle_item="file_name",
        niveau_log=logging.ERROR,
    )
    def travail(item):
        yield {"i": 0}
        yield {"i": 1}
        raise RuntimeError("boom au 3e")

    with caplog.at_level(logging.ERROR):
        produits = list(travail({"file_name": "z.zip"}, stats))  # ne lève pas

    assert len(produits) == 2  # les k=2 documents d'avant la levée sont bien émis
    assert stats.extraits == 2 and stats.echecs_extraction == 1
    assert "extraction" in caplog.text and "z.zip" in caplog.text


def test_exception_hors_classe_capturee_propage():
    """`capture=` est resserré par étage (decrypt n'attrape que `ValueError`). Une exception
    **hors** de cette classe (ici `TypeError`) n'est PAS avalée : elle propage, et l'échec
    n'est pas compté — sinon on masquerait des bugs comme une erreur de lecture SFTP."""
    stats = StatsChaine()

    @etape_chaine(
        succes="dechiffres",
        echec="echecs_dechiffrement",
        libelle="déchiffrement",
        cle_item="file_name",
        capture=ValueError,
    )
    def travail(item):
        raise TypeError("pas une ValueError")
        yield  # générateur

    with pytest.raises(TypeError, match="pas une ValueError"):
        list(travail({"file_name": "z.zip"}, stats))

    assert stats.echecs_dechiffrement == 0  # non compté : ce n'est pas un échec de la discipline
