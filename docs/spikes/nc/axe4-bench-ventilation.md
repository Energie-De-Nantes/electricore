# Axe 4 — bench de ventilation calendaire par oracle

> Généré par `docs/spikes/nc/axe4_bench_ventilation.py` — issue [#547](https://github.com/Energie-De-Nantes/electricore/issues/547), PRD [#542](https://github.com/Energie-De-Nantes/electricore/issues/542).
>
> Uniquement des agrégats anonymes — aucun PDL, RSC ni num_compteur. Le détail par PDL est une sortie locale non committée (`sorties-locales/axe4_bench_detail_mois.csv`).

## Principe de l'oracle

On ne peut pas juger la ventilation d'un non-communicant (NC) : pas de vérité terrain.
L'oracle emprunte des PDL **communicants** (niveau d'ouverture 2) qui ont **à la fois**
leurs fenêtres moisniversaire F15 (le « matériau NC ») **et** un mesuré quotidien
R64/R151 (la vérité, indépendante de la reconstruction). On les reconstruit « comme s'ils
étaient NC » depuis les seules fenêtres, on compare au mesuré. Aucune fenêtre synthétique.

**Sélection oracle (source de biais nommée)** : un PDL entre dans le bench s'il a ≥ 6
fenêtres F15 cycliques *propres*. Gate qualité : tout PDL dont une fenêtre F15 diffère de
l'index brut à ses bornes de plus de max(100 kWh, 30 %) est **entièrement écarté**
(remplacement de compteur ou correction Enedis — confondre ça avec une erreur de
ventilation fausserait le bench). Population retenue : **746 PDL oracle**,
**7021 mois** comparables (vérité + couverture NC complète).

## Résultats — erreur de reconstruction

### Grain mois calendaire (ADR-0026)

| Méthode | n | MAE (kWh) | biais (kWh) | \|err\| médian | p90 |
|---|--:|--:|--:|--:|--:|
| 1. Prorata temporis | 7021 | 154.2 | -136.8 | 5.75 kWh (3.6%) | 43.99 kWh (14.1%) |
| 2. Profil saisonnier parc | 7021 | 179.0 | -134.5 | 16.91 kWh (9.5%) | 104.01 kWh (47.1%) |

### Grain trimestre (accise)

| Méthode | n | MAE (kWh) | biais (kWh) | \|err\| médian | p90 |
|---|--:|--:|--:|--:|--:|
| 1. Prorata temporis | 1335 | 28.5 | -3.3 | 8.36 kWh (1.7%) | 80.32 kWh (7.4%) |
| 2. Profil saisonnier parc | 1335 | 45.6 | -15.8 | 15.91 kWh (2.8%) | 115.43 kWh (14.0%) |

> **Lire MAE vs médiane** : la MAE en kWh est tirée par une queue de gros consommateurs
> (C4 > 36 kVA) et par les résidus F15-vs-index que le gate ne retire pas tous ; la
> **médiane** et l'**erreur relative** (entre parenthèses) disent le signal pratique.

## Verdict — « le prorata ventile mal la saisonnalité »

**INVALIDÉ — le prorata ventile au moins aussi bien que le profil saisonnier.**

- Mois : prorata |err| médian **5.75 kWh** vs profil saisonnier **16.91 kWh**.
- Trimestre : prorata **8.36 kWh** vs profil **15.91 kWh**.

Sur ce parc, le profil saisonnier du parc EDN (leave-one-out) **ne bat pas** le prorata :
la saisonnalité *intra-fenêtre* (fenêtres ~30 j) est trop courte pour que la modulation
mensuelle paie. Le prorata reste la baseline à battre.

## Praticabilité « sans-ventilation aux bornes réelles » (méthode 3)

- **Fréquence des relevés réels** qui borneraient un solde propre chez les NC : mesurée par
  l'**axe 2 #545** (rare, ~supra-annuel). Non recalculée ici — l'oracle est communicant
  (index R151/R64 quotidien), sa cadence ne renseigne pas les vrais NC.
- **Taille des fenêtres moisniversaire** (grain naturel du solde) : p50 **30 j**, p90 **31 j**.
- **Solde au mois civil** : impraticable — ces fenêtres (~30 j) ne tombent quasi jamais
  entièrement dans un mois civil, d'où **100%** d'énergie non soldable sans ventiler une
  fenêtre à cheval.
- **Solde au trimestre** (grain accise) : **35%** médian de l'énergie reste à estimer aux
  bords (p90 **81%**) — le solde « propre » n'annule pas le besoin d'estimer les bouts.

- **Méthode 4 (profils officiels Enedis)** : benchée si `coefficients_profils_enedis.csv` est fourni (chargeur pluggable prêt). Extrait public non embarqué dans ce run — cf. §6 du script.

## Limites

- Oracle = communicants ; leur profil de conso peut différer des vrais NC (parc pré-Linky,
  cf. axe 1 #544). Biais nommé, non corrigé.
- MAE inflatée par les gros consommateurs et les résidus F15-vs-index ; médiane + relatif
  privilégiés pour le verdict.
- R67 absent (campagne #543) : le bench s'appuie sur F15 comme matériau moisniversaire.
