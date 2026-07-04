# ADR-0054 — Convention date-ancre pour la sélection des factures d'une campagne

## Statut

Accepté (#561, PR #562).

## Contexte

Le rapprochement facturation (`lignes_factures_du_mois`, `electricore/integrations/odoo/sources.py`)
croise les quantités Enedis du mois M avec les lignes de facture Odoo du mois M. La
sélection initiale filtrait par fenêtre : `invoice_date ∈ [M, M+1)`.

Deux faits Odoo rendent cette fenêtre bogée :

- Les brouillons de campagne (`account.move.state = draft`) naissent **sans**
  `invoice_date` — cette date ne se pose qu'à la validation de la facture. Une
  fenêtre par date ne peut donc jamais les voir : la campagne en cours de saisie
  était invisible du rapprochement tant qu'elle n'était pas validée.
- Une fois la campagne validée, `invoice_date` est en réalité posée à **une seule
  et même date** pour toutes les factures de la campagne — pas répartie dans le
  mois. La fenêtre était donc une approximation inutilement large d'une valeur en
  fait unique.

## Décision

**Sélection par égalité stricte sur une date-ancre**, pas par fenêtre :

- Odoo pose `invoice_date = 05/(M+1)` sur **tous** les brouillons de la campagne
  du mois M, au moment de « lancer la facturation du mois » (conso de juin →
  factures datées 05/07). Rollover décembre : conso de décembre N → `05/01/(N+1)`.
- `date_ancre(mois)` (`electricore/integrations/odoo/sources.py`) calcule cette
  date. `lignes_factures_du_mois` sélectionne par `invoice_date == date_ancre(mois)` —
  une clé, plus de fenêtre.
- **Une seule campagne par mois** : pas de distinction cycle automatique / campagne
  manuelle, une seule date-ancre à surveiller.

C'est une **convention inter-systèmes**, pas un détail d'implémentation local :
Odoo la pose à la génération, electricore la lit. Si l'un des deux bascule l'ancre
sans que l'autre suive, la sélection casse **silencieusement** (une requête sur la
mauvaise date ne renvoie simplement rien, ou renvoie l'ancienne campagne). D'où le
check pré-campagne côté vérifications Odoo (#564, `electricore/integrations/odoo/verification.py`)
qui détecte, avant chaque lancement de campagne, tout brouillon de commande énergie
daté hors de l'ancre courante ou sans date — la violation de convention devient
visible dans `/check odoo` au lieu d'être absorbée en silence par la sélection.

### Alternatives écartées

- **Fenêtre `[M, M+1)`** (comportement initial) : n'a jamais vu les brouillons de
  campagne (ceux-ci naissent sans date) — c'est le bug d'origine (#561).
- **« Fenêtre OU brouillon » (palliatif)** : rendait la campagne courante visible
  en avalant tout `state = draft`, y compris un brouillon d'une campagne
  antérieure resté traînant, ou un brouillon hors énergie — pas de garde-fou sur
  la convention de date, juste un filtre plus permissif.
- **Antidatage** (poser `invoice_date` au 30/06 au lieu du 05/07) : déplace le
  problème sans le résoudre — reste une date arbitraire à synchroniser entre les
  deux systèmes.
- **Poser la date à l'injection** (côté electricore plutôt qu'Odoo) : Odoo est le
  système qui génère la campagne ; il est le seul à savoir quand elle a réellement
  eu lieu.
- **Heuristique « dernière facture »** : fragile dès que deux campagnes
  coexistent (rattrapage, re-génération) — pas de garantie d'ordre stable.

## Conséquences

- Un seul calcul d'ancre (`date_ancre`), réutilisé par la sélection (#561) et par
  le check pré-campagne (#564) — pas de risque de dérive entre les deux.
- La convention doit être documentée côté Odoo (génération de la campagne) autant
  que côté electricore : un changement de l'un sans l'autre casse silencieusement,
  d'où le check pré-campagne en garde-fou plutôt qu'une simple note de documentation.
- Un brouillon sans date, ou daté ailleurs qu'à l'ancre courante, n'est plus
  ramassé du tout par `lignes_factures_du_mois` — il doit être corrigé à la
  source (Odoo) avant de pouvoir être rapproché.
