#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Author: Komal Thareja (kthare10@renci.org)
"""
Hardened ``pickle`` loading.

FABRIC actors persist domain objects (Slices, Reservations, Units, Delegations,
POAs, ...) as pickles in Postgres and carry pickled slivers over Kafka. Plain
``pickle.loads`` on such data means anyone able to write the store or the bus can
execute arbitrary code, because pickle can import and call any global on load.

``restricted_loads`` closes the practical remote-code-execution vectors by
refusing to resolve the standard "gadget" modules/callables (``os``,
``subprocess``, ``builtins.eval``/``exec``, ...) during unpickling, while
allowing the FABRIC domain modules and the ordinary stdlib types that legitimate
pickled objects contain.

Rollout policy (interim hardening — the storage format is unchanged):

* Dangerous globals are **always** blocked.
* Known-good modules (FABRIC packages + a curated safe-stdlib set) are allowed
  silently.
* Any other module is, by default, **allowed but logged at WARNING** so that a
  too-narrow allowlist can never break loading of legitimate production data.
  Set ``ENFORCE = True`` (or env ``FABRIC_PICKLE_ENFORCE=1``) after confirming
  from the audit logs that no legitimate class is being flagged, to switch to a
  strict allowlist that raises on anything unlisted.
"""
import io
import logging
import os
import pickle
from typing import Any

logger = logging.getLogger(__name__)

# Switch to strict allowlist enforcement once audit logs are clean.
ENFORCE: bool = os.environ.get("FABRIC_PICKLE_ENFORCE", "").lower() in ("1", "true", "yes")

# Standard pickle remote-code-execution / exfiltration gadget modules. These
# never appear in legitimate FABRIC domain pickles, so denying them outright
# closes the RCE vectors without risking breakage of real data.
_DENIED_MODULES = frozenset({
    "os", "nt", "posix", "subprocess", "sys", "socket", "ssl", "shutil",
    "ctypes", "cffi", "importlib", "imp", "pty", "platform", "multiprocessing",
    "asyncio", "code", "codeop", "pdb", "bdb", "runpy", "marshal", "pickle",
    "_pickle", "commands", "popen2", "webbrowser", "antigravity", "signal",
    "smtplib", "ftplib", "telnetlib", "requests", "urllib", "http",
})

# Dangerous callables inside otherwise-common modules. Kept intentionally narrow:
# getattr/setattr etc. are deliberately NOT blocked because they appear in
# legitimate __reduce__ output, and their dangerous *targets* (os, importlib,
# ...) are already denied above.
_DENIED_QUALIFIED = frozenset({
    ("builtins", "eval"), ("builtins", "exec"), ("builtins", "compile"),
    ("builtins", "open"), ("builtins", "__import__"), ("builtins", "breakpoint"),
    ("builtins", "input"),
})

# Known-good top-level modules for FABRIC domain / sliver objects, plus the
# ordinary stdlib types that appear inside them (determined empirically).
_ALLOWED_MODULES = frozenset({
    # FABRIC / model packages
    "fabric_cf", "fim", "fabric_mb", "fabrictestbed", "fss_utils", "fss",
    # safe stdlib types found in the domain object graphs
    "builtins", "datetime", "uuid", "logging", "enum", "collections",
    "_collections", "collections.abc", "decimal", "ipaddress", "copyreg",
    "copy_reg", "typing", "numbers", "re", "functools", "operator",
    # graph library used by fim slivers/topologies
    "networkx",
})


class RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that blocks code-execution gadgets during ``find_class``."""

    def find_class(self, module: str, name: str) -> Any:
        top = module.split(".", 1)[0]

        if top in _DENIED_MODULES or (module, name) in _DENIED_QUALIFIED:
            raise pickle.UnpicklingError(
                f"Refusing to load unsafe pickle global {module}.{name}")

        if top in _ALLOWED_MODULES:
            return super().find_class(module, name)

        # Unlisted module: audit by default, enforce on demand.
        if ENFORCE:
            raise pickle.UnpicklingError(
                f"Refusing to load unlisted pickle global {module}.{name}")
        logger.warning("restricted_unpickler: allowing unlisted pickle global %s.%s "
                       "(audit; add to the allowlist before enabling ENFORCE)", module, name)
        return super().find_class(module, name)


def restricted_loads(data: bytes) -> Any:
    """Drop-in replacement for ``pickle.loads`` that blocks unsafe globals."""
    return RestrictedUnpickler(io.BytesIO(data)).load()
