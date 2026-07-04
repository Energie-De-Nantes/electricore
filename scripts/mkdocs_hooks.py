"""Hook MkDocs (natif, sans dépendance) — tolère les liens déjà connus des registres figés.

`docs/adr/**` (contenu immuable — une fois publié, un ADR ne se retouche pas, pas même
pour un lien) et `docs/agents/**` (registres agent, hors périmètre de ce chantier)
référencent souvent du code ou des fichiers hors `docs/` (CONTEXT.md, CONTRIBUTING.md,
scripts de déploiement…), ou pour deux ADRs, une coquille de nommage vers un autre ADR.
MkDocs classe tout ça en liens « non résolus » — mais retoucher le contenu de ces
fichiers rien que pour faire taire le validateur est interdit (#560).

On démote donc les warnings de validation de lien dont la PAGE SOURCE vit dans un de ces
deux répertoires. Tout le reste — y compris un nouveau lien mort introduit ailleurs,
ou dans ces répertoires eux-mêmes via un futur canal qui ne serait pas ce filtre —
reste bloquant sous `--strict`.

ponytail: filtre pinné au libellé mkdocs actuel ("Doc file '<src>' contains a link...").
Si mkdocs change ce libellé, le filtre cesse de matcher et le strict redevient bloquant
partout — échec sûr (trop de warnings), jamais silencieux.
"""

import logging
import re

_PAGE_TOLEREE = re.compile(r"^Doc file '(?:adr|agents)/")


def _tolerer_registres_figes(record: logging.LogRecord) -> bool:
    if record.levelno == logging.WARNING and _PAGE_TOLEREE.match(record.getMessage()):
        record.levelno = logging.INFO
        record.levelname = "INFO"
    return True


def on_startup(*, command: str, dirty: bool) -> None:
    # Un logging.Filter attaché à un logger ne s'applique qu'aux appels faits SUR ce
    # logger précis (pas ceux des enfants qui remontent par les handlers) : la
    # validation de liens vit dans `mkdocs.structure.pages`, donc c'est là qu'on
    # branche le filtre, pas sur le logger racine `mkdocs`.
    logging.getLogger("mkdocs.structure.pages").addFilter(_tolerer_registres_figes)
