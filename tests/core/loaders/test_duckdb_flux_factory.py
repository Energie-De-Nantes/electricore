"""Tests de la factory `flux(nom)` et de l'erreur `FluxInconnu` (#272).

La factory `flux(nom)` résout un flux Enedis enregistré (`FLUX_DESCRIPTORS`) en
`DuckDBQuery` ; un nom non enregistré lève `FluxInconnu` (sous-classe de
`ValueError`) — l'interface du loader porte la connaissance du registre, le
routeur n'a plus à l'atteindre. Portée : les 5 flux Enedis uniquement ;
`releves()` reste hors périmètre (modèle dbt canonique, ADR-0029).
"""

import pytest

from electricore.core.loaders.duckdb import DuckDBQuery, FluxInconnu, flux
from electricore.core.loaders.duckdb.registry import FLUX_DESCRIPTORS


class TestFluxFactory:
    """`flux(nom)` produit un builder pour chaque flux enregistré."""

    @pytest.mark.parametrize("nom", ["c15", "r151", "r15", "r15_acc", "f15_detail", "f12_detail", "r64", "r67"])
    def test_flux_retourne_un_builder(self, nom):
        assert isinstance(flux(nom), DuckDBQuery)

    def test_flux_propage_le_database_path(self):
        """Le chemin de base est transmis au builder (matérialisation paresseuse)."""
        query = flux("c15", database_path="nonexistent_test.duckdb")
        assert isinstance(query, DuckDBQuery)
        with pytest.raises(FileNotFoundError):
            query.lazy()

    def test_flux_reste_chainable(self):
        query = flux("r151").filter({"pdl": ["PDL123"]}).limit(10)
        assert isinstance(query, DuckDBQuery)


class TestFluxInconnu:
    """Un nom non enregistré échoue au seam, en nommant les flux disponibles."""

    def test_nom_inconnu_leve_flux_inconnu(self):
        with pytest.raises(FluxInconnu):
            flux("totalement_inexistant")

    def test_flux_inconnu_est_une_value_error(self):
        """Le routeur peut le mapper sur un 404 sans dépendre d'un type maison fort."""
        assert issubclass(FluxInconnu, ValueError)

    def test_le_message_nomme_les_flux_disponibles(self):
        with pytest.raises(FluxInconnu) as excinfo:
            flux("totalement_inexistant")
        message = str(excinfo.value)
        for nom in FLUX_DESCRIPTORS:
            assert nom in message

    def test_flux_inconnu_porte_le_nom_et_la_liste(self):
        """Attributs structurés pour un consommateur qui ne veut pas parser le message."""
        with pytest.raises(FluxInconnu) as excinfo:
            flux("totalement_inexistant")
        erreur = excinfo.value
        assert erreur.nom == "totalement_inexistant"
        assert sorted(FLUX_DESCRIPTORS) == list(erreur.disponibles)


class TestPorteeFluxFactory:
    """`releves` n'est pas un flux Enedis enregistré : hors périmètre (ADR-0029)."""

    def test_releves_n_est_pas_routable_par_flux(self):
        with pytest.raises(FluxInconnu):
            flux("releves")


class TestValidateursSeams:
    """Le descripteur d'un flux consommé par un pipeline-cœur porte son contrat
    Pandera au seam (ADR-0035 §5). #295 ferme la dernière lacune de registre :
    `affaires` (consommé par `affaires_ouvertes`) gagne `AffaireJalon`."""

    def test_descriptor_affaires_porte_affaire_jalon(self):
        from electricore.core.loaders.duckdb.registry import DESCRIPTOR_AFFAIRES
        from electricore.core.models.affaire_jalon import AffaireJalon

        assert DESCRIPTOR_AFFAIRES.validator is AffaireJalon

    def test_descriptor_f15_sans_validator_par_audit(self):
        """F15 n'a **pas** de seam de consommation typé (#295, ADR-0035 §5) : son unique
        consommateur (`contexte_mensuel.documents()`) le filtre puis le verse tel quel dans
        des onglets XLSX — pas de calcul sur sa forme. Pas de miroir Pandera là où aucun
        pipeline cœur ne consomme le flux pour un calcul (≠ R151/R15→énergie). Garde-fou : si
        un futur calcul typé branche F15 (ex. rapprochement TURPE↔F15, #468), ce test rappelle
        d'ajouter le contrat au lieu de laisser passer une lacune de registre."""
        from electricore.core.loaders.duckdb.registry import DESCRIPTOR_F15

        assert DESCRIPTOR_F15.validator is None

    def test_descriptor_f12_sans_validator_meme_raisonnement_que_f15(self):
        """F12 (#536, symétrie de registre) : même raisonnement que F15 — pas de seam de
        calcul core établi, pas de miroir Pandera à ce stade."""
        from electricore.core.loaders.duckdb.registry import DESCRIPTOR_F12

        assert DESCRIPTOR_F12.validator is None
        assert DESCRIPTOR_F12.table == "flux_enedis.flux_f12_detail"


class TestFactoriesNommeesEquivalentesAFlux:
    """#273 : c15()…r64() produisent la même requête que `flux(nom)`.

    On teste l'**équivalence observable** (la requête SQL bâtie), pas le mécanisme
    de délégation — qu'une factory nommée appelle `flux()` ou `make_query()` est de
    l'implémentation ; ce qui compte, et ce qui survit à un refactor, c'est qu'elle
    cible le même flux. Le collapse #273 doit préserver cette équivalence.
    """

    @pytest.mark.parametrize("nom", ["c15", "r151", "r15", "r15_acc", "f15_detail", "f12_detail", "r64", "r67"])
    def test_factory_nommee_batit_la_meme_requete_que_flux(self, nom):
        from electricore.core.loaders.duckdb import c15, f12_detail, f15_detail, r15, r15_acc, r64, r67, r151

        nommee = {
            "c15": c15,
            "r151": r151,
            "r15": r15,
            "r15_acc": r15_acc,
            "f15_detail": f15_detail,
            "f12_detail": f12_detail,
            "r64": r64,
            "r67": r67,
        }[nom]
        assert nommee()._build_final_query() == flux(nom)._build_final_query()
