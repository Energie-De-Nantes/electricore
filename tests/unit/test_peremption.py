"""
Tests du check de péremption des taux régulés (issue #186, ADR-0024).

Les rythmes attendus sont des heuristiques (TURPE au 1ᵉʳ août, Accise au
1ᵉʳ janvier, CTA sans rythme connu) : un warning, jamais d'auto-correction.
"""

import datetime as dt

from electricore.core.peremption import avertissements_peremption


class TestAvertissementsPeremption:
    def test_a_jour_aucun_avertissement(self):
        """Chaque taxe à rythme a une ligne postérieure à son dernier jalon → rien à signaler."""
        avertissements = avertissements_peremption(
            dt.date(2026, 7, 1),
            derniers_starts={"TURPE": dt.date(2025, 8, 1), "Accise": dt.date(2026, 1, 1)},
        )

        assert avertissements == ()

    def test_turpe_en_retard_apres_le_premier_aout(self):
        """Au 2 août, la grille attendue du 1ᵉʳ août manque → avertissement TURPE."""
        avertissements = avertissements_peremption(
            dt.date(2026, 8, 2),
            derniers_starts={"TURPE": dt.date(2025, 8, 1), "Accise": dt.date(2026, 1, 1)},
        )

        (a,) = avertissements
        assert a.taxe == "TURPE"
        assert a.attendu_depuis == dt.date(2026, 8, 1)
        assert a.dernier_start == dt.date(2025, 8, 1)
        assert "délibération CRE" in a.consigne

    def test_accise_en_retard_apres_le_premier_janvier(self):
        """Loi de finances attendue au 1ᵉʳ janvier : ligne absente → avertissement Accise."""
        avertissements = avertissements_peremption(
            dt.date(2026, 6, 13),
            derniers_starts={"TURPE": dt.date(2025, 8, 1), "Accise": dt.date(2025, 8, 1)},
        )

        (a,) = avertissements
        assert a.taxe == "Accise"
        assert a.attendu_depuis == dt.date(2026, 1, 1)
        assert "loi de finances" in a.consigne

    def test_cta_sans_rythme_jamais_signalee(self):
        """Pas de rythme connu pour la CTA : pas de check, même très ancienne."""
        avertissements = avertissements_peremption(
            dt.date(2030, 6, 1),
            derniers_starts={"TURPE": dt.date(2029, 8, 1), "Accise": dt.date(2030, 1, 1), "CTA": dt.date(2020, 1, 1)},
        )

        assert avertissements == ()

    def test_message_actionnable(self):
        """Le message dit quoi vérifier, avec les deux dates qui situent le retard."""
        (a,) = avertissements_peremption(
            dt.date(2026, 8, 2),
            derniers_starts={"TURPE": dt.date(2025, 8, 1), "Accise": dt.date(2026, 1, 1)},
        )

        assert "TURPE" in a.message
        assert "2026-08-01" in a.message and "2025-08-01" in a.message
        assert "vérifier" in a.message


class TestDerniersStartsReels:
    def test_les_csv_versionnes_sont_verifiables(self):
        """Smoke : le check tourne sur les vrais CSV sans exploser, et ne parle que des taxes à rythme."""
        avertissements = avertissements_peremption(dt.date(2026, 6, 13))

        assert all(a.taxe in ("TURPE", "Accise") for a in avertissements)
