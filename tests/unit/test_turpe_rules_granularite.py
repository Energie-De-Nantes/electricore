"""Garde-fou de l'invariant **load-bearing** du calculateur turpe-variable (ADR-0030, #252).

Le calculateur passe **les 7 cadrans** et laisse la sommation arbitrer : c'est correct
*uniquement si* chaque règle FTA porte des coefficients `c_*` non-nuls à **au plus une**
granularité (`base`, `hp_hc`, ou `4_cadrans`). Si une règle portait deux granularités
non-nulles, envoyer les 7 énergies **double-compterait** en silence.

État vérifié au 2026-06-15 : 38 règles (26 `4_cadrans`, 8 `base`, 4 `hp_hc`), zéro
chevauchement. Ce test fige l'invariant : il casse (avec la FTA fautive nommée) si une
future ligne de `turpe_rules.csv` le viole.
"""

from electricore.core.pipelines.turpe import load_turpe_rules

# Groupes de cadrans explicites (nomenclature CRE c_*).
GROUPES_CADRANS: dict[str, tuple[str, ...]] = {
    "base": ("c_base",),
    "hp_hc": ("c_hp", "c_hc"),
    "4_cadrans": ("c_hph", "c_hpb", "c_hch", "c_hcb"),
}


def granularites_non_nulles(regle: dict) -> set[str]:
    """Granularités dont **au moins un** coefficient `c_*` est non-null **et** ≠ 0."""
    return {
        granularite
        for granularite, coeffs in GROUPES_CADRANS.items()
        if any(regle[c] is not None and regle[c] != 0 for c in coeffs)
    }


def test_chaque_fta_une_seule_granularite_non_nulle():
    """Chaque règle de `turpe_rules.csv` a au plus une granularité de cadrans non-nulle."""
    regles = load_turpe_rules().collect()

    violations = [
        (regle["Formule_Tarifaire_Acheminement"], regle["start"], sorted(grans))
        for regle in regles.to_dicts()
        if len(grans := granularites_non_nulles(regle)) > 1
    ]

    assert not violations, (
        "Invariant ADR-0030 violé — règle(s) avec plusieurs granularités non-nulles "
        "(le calculateur turpe-variable double-compterait) :\n"
        + "\n".join(f"  - FTA {fta} (start={start}) : granularités {grans}" for fta, start, grans in violations)
    )


def test_le_garde_detecte_une_violation_synthetique():
    """Preuve que le garde échouerait : une règle à 2 granularités est bien flaggée."""
    regle_fautive = dict.fromkeys(("c_base", "c_hp", "c_hc", "c_hph", "c_hpb", "c_hch", "c_hcb"), 0.0)
    regle_fautive["c_base"] = 1.25  # base
    regle_fautive["c_hp"] = 4.94  # ... ET hp_hc → double granularité

    assert granularites_non_nulles(regle_fautive) == {"base", "hp_hc"}


def test_chaque_fta_a_bien_une_granularite():
    """Corollaire : aucune règle n'est *sans* granularité (toutes valorisent une énergie)."""
    regles = load_turpe_rules().collect()
    sans_granularite = [
        (regle["Formule_Tarifaire_Acheminement"], regle["start"])
        for regle in regles.to_dicts()
        if len(granularites_non_nulles(regle)) == 0
    ]
    assert not sans_granularite, f"Règle(s) sans aucun coefficient c_* non-nul : {sans_granularite}"
