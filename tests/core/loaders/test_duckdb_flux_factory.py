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

    @pytest.mark.parametrize("nom", ["c15", "r151", "r15", "f15", "r64"])
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


class TestFactoriesNommeesEquivalentesAFlux:
    """#273 : c15()…r64() produisent la même requête que `flux(nom)`.

    On teste l'**équivalence observable** (la requête SQL bâtie), pas le mécanisme
    de délégation — qu'une factory nommée appelle `flux()` ou `make_query()` est de
    l'implémentation ; ce qui compte, et ce qui survit à un refactor, c'est qu'elle
    cible le même flux. Le collapse #273 doit préserver cette équivalence.
    """

    @pytest.mark.parametrize("nom", ["c15", "r151", "r15", "f15", "r64"])
    def test_factory_nommee_batit_la_meme_requete_que_flux(self, nom):
        from electricore.core.loaders.duckdb import c15, f15, r15, r64, r151

        nommee = {"c15": c15, "r151": r151, "r15": r15, "f15": f15, "r64": r64}[nom]
        assert nommee()._build_final_query() == flux(nom)._build_final_query()
