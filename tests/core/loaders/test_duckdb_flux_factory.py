"""Tests de la factory `flux(nom)` et de l'erreur `FluxInconnu` (#272).

La factory `flux(nom)` résout un flux Enedis enregistré (`FLUX_CONFIGS`) en
`DuckDBQuery` ; un nom non enregistré lève `FluxInconnu` (sous-classe de
`ValueError`) — l'interface du loader porte la connaissance du registre, le
routeur n'a plus à l'atteindre. Portée : les 5 flux Enedis uniquement ;
`releves()` reste hors périmètre (modèle dbt canonique, ADR-0029).
"""

import pytest

from electricore.core.loaders.duckdb import DuckDBQuery, FluxInconnu, flux
from electricore.core.loaders.duckdb.registry import FLUX_CONFIGS


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
        for nom in FLUX_CONFIGS:
            assert nom in message

    def test_flux_inconnu_porte_le_nom_et_la_liste(self):
        """Attributs structurés pour un consommateur qui ne veut pas parser le message."""
        with pytest.raises(FluxInconnu) as excinfo:
            flux("totalement_inexistant")
        erreur = excinfo.value
        assert erreur.nom == "totalement_inexistant"
        assert sorted(FLUX_CONFIGS) == list(erreur.disponibles)


class TestPorteeFluxFactory:
    """`releves` n'est pas un flux Enedis enregistré : hors périmètre (ADR-0029)."""

    def test_releves_n_est_pas_routable_par_flux(self):
        with pytest.raises(FluxInconnu):
            flux("releves")


class TestFactoriesNommeesDeleguent:
    """#273 : c15()…r64() délèguent à `flux()` — une seule résolution registre."""

    @pytest.mark.parametrize("nom", ["c15", "r151", "r15", "f15", "r64"])
    def test_la_factory_nommee_route_la_meme_config_que_flux(self, nom):
        from electricore.core.loaders.duckdb import c15, f15, r15, r64, r151

        nommee = {"c15": c15, "r151": r151, "r15": r15, "f15": f15, "r64": r64}[nom]
        # Même entrée de registre que la résolution dynamique : preuve de la délégation.
        assert nommee().config is flux(nom).config
