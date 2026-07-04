# Charte de la documentation

ElectriCore est un commun ([ADR-0024](../adr/0024-trois-registres-de-savoir.md) :
« EDN est le premier prototype d'une fédération de communs de l'électricité »).
Sa documentation s'adresse à plusieurs publics, pas aux seuls contributeurs de
code — cette charte fixe qui ils sont et comment on écrit pour eux.
Décision complète : [ADR-0054](../adr/0054-chemin-documentation-roles-escalier.md).

## Les 5 rôles de lecture

| Rôle | Ce qu'iel vient chercher | Sa section |
|---|---|---|
| **Usager·ère** | Comprendre ce qu'ElectriCore calcule et pourquoi, sans code | [Comprendre](../comprendre/index.md) |
| **Opérateur·ice** | Facturer au quotidien avec le bot et les notebooks | [Facturer](../facturiste/changelog-facturiste.md) |
| **Déployeur·euse** | Installer et administrer une instance | [Déployer](../deployer/index.md) |
| **Contributeur·ice** | Coder en amont du projet | [Contribuer](index.md) |
| **Mainteneur·e** | Tenir l'architecture, les releases, le réglementaire | [Maintenir](../maintenir/index.md) |

Ce découpage vit **ici**, pas dans le glossaire métier
([CONTEXT-MAP.md](https://github.com/Energie-De-Nantes/electricore/blob/main/CONTEXT-MAP.md)) :
les rôles de lecture sont un concept de documentation, pas un concept métier Enedis.

## Le principe d'escalier

Un sujet transverse (le TURPE, par exemple) existe à plusieurs étages du site, et
chaque étage raconte autre chose :

1. **Comprendre** — quoi, en une phrase pour un public non technique.
2. **Facturer** — comment s'en servir au quotidien.
3. **Contribuer / Déployer** — comment c'est fait, comment l'installer.
4. **Maintenir** — comment le faire évoluer, l'historique de la décision.

La duplication entre étages est **assumée** — chaque étage doit se lire seul,
sans renvoyer le lecteur ailleurs pour finir sa phrase — mais **bornée** par des
**passerelles montantes** : chaque page pointe, en pied de page, vers l'étage du
dessus pour qui veut creuser plus loin. Une page ne redescend jamais.

## Charte éditoriale

- **Point médian partout** dans le texte courant qui s'adresse à un rôle :
  usager·ère, opérateur·ice, déployeur·euse, contributeur·ice, mainteneur·e.
- **Tutoiement** dans les guides et les pages d'orientation (accueil, pages
  d'index de section, cette charte) — on s'adresse à une personne.
- **Tournures impersonnelles** dans la référence technique (ADR, docstrings,
  contrats d'API, formats de données) — on décrit un système, pas une personne.
- **Domaine métier en français** ; le vocabulaire reste celui du glossaire
  ([CONTEXT-MAP.md](https://github.com/Energie-De-Nantes/electricore/blob/main/CONTEXT-MAP.md)),
  jamais redéfini ici.

[Retour à l'accueil](../index.md).
