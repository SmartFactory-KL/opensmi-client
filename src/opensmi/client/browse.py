# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Browsing related options and helpers."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Flag, auto
from pathlib import PurePosixPath
from typing import Final, TypeAlias

BrowseMatcher: TypeAlias = Callable[[PurePosixPath], bool]


class BrowseFeature(Flag):
    """What to browse on a remote UA object."""

    MACHINES = auto()
    COMPONENTS = auto()
    LOCK = auto()
    RESOURCES = auto()
    USERS = auto()

    SKILL_SET = auto()
    METHOD_SET = auto()

    # variable containers
    ATTRIBUTES = auto()
    IDENTIFICATION = auto()
    MONITORING = auto()
    PARAMETER_SET = auto()
    FINAL_RESULT_DATA = auto()


@dataclass(frozen=True, slots=True)
class BrowseRule:
    """Select which features to browse for matching objects.

    Rules are evaluated against each object during recursive browsing.
    """

    match: BrowseMatcher
    features: BrowseFeature


def browse_when(match: BrowseMatcher, *features: BrowseFeature) -> BrowseRule:
    """Create a browse rule that applies features when a path matches.

    Multiple features are combined into a single :class:`BrowseFeature`
    using bitwise OR.
    """
    combined = BrowseFeature(0)
    for feature in features:
        combined |= feature
    return BrowseRule(match, combined)


def evaluate_browse_features(rules: Iterable[BrowseRule], path: PurePosixPath) -> BrowseFeature:
    """Evaluate the browse features applicable to an object.

    All matching rules contribute their features, which are combined
    using bitwise OR. Returns ``BrowseFeature(0)`` when no rules match.
    """
    features = BrowseFeature(0)
    for rule in rules:
        if rule.match(path):
            features |= rule.features
    return features


# ------------------------------------------------------------------------------------------------------------
# Helpers for defining rules
# ------------------------------------------------------------------------------------------------------------


def match_all(_path: PurePosixPath) -> bool:
    """Match every object."""
    return True


def match_name(name: str) -> BrowseMatcher:
    """Match objects by name."""
    return lambda path: path.name == name


def match_path(path: PurePosixPath | str) -> BrowseMatcher:
    """Match an exact path."""
    path = PurePosixPath(path)
    return lambda current: current == path


def match_path_prefix(prefix: PurePosixPath | str) -> BrowseMatcher:
    """Match a path and all of its descendants."""
    prefix = PurePosixPath(prefix)
    return lambda path: path == prefix or prefix in path.parents


def match_any(*matches: BrowseMatcher) -> BrowseMatcher:
    """Match when any of the given predicates match."""
    return lambda path: any(match(path) for match in matches)


def match_all_of(*matches: BrowseMatcher) -> BrowseMatcher:
    """Match when all the given predicates match."""
    return lambda path: all(match(path) for match in matches)


def match_not(match: BrowseMatcher) -> BrowseMatcher:
    """Negate a match predicate."""
    return lambda path: not match(path)


DEFAULT_BROWSE_RULE: Final[BrowseRule] = BrowseRule(
    match=match_all,
    features=~BrowseFeature(0),  # all defined features
)
