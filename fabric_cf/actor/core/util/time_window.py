#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, List, Optional, Tuple

TimeWindow = Optional[Tuple[Optional[datetime], Optional[datetime]]]


def to_utc(value: Optional[datetime]) -> Optional[datetime]:
    """
    Treat naive datetimes as UTC so they can be compared with timezone aware ones
    :param value: datetime
    :return: timezone aware datetime
    """
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def usage_at_peaks(*, allocations: Iterable[Tuple[TimeWindow, Any]], start: Optional[datetime],
                   end: Optional[datetime], zero: Any, add: Callable[[Any, Any], Any] = lambda x, y: x + y) -> List[Any]:
    """
    Compute the usage at each point within [start, end) where usage may peak.

    Usage only changes when an allocation starts or ends, so the peak is at the start of the window or at the
    start of an allocation within the window. Allocations with an unknown window (None, or missing start/end)
    are considered to be held throughout. If start or end is not specified, all allocations are summed.

    :param allocations: (window, value) pairs; window is (start, end) during which the value is held
    :param start: start of the window of interest
    :param end: end of the window of interest
    :param zero: value representing no usage
    :param add: function to add two values
    :return: list of usages at candidate peak points; caller checks each (e.g. max for scalars, or each
             dimension for multi-dimensional capacities)
    """
    allocations = [(None if w is None else (to_utc(w[0]), to_utc(w[1])), v) for w, v in allocations]
    start = to_utc(start)
    end = to_utc(end)

    if start is None or end is None:
        points = [None]
    else:
        points = {start}
        for window, _ in allocations:
            if window is not None and window[0] is not None and start < window[0] < end:
                points.add(window[0])
        points = sorted(points)

    usages = []
    for point in points:
        used = zero
        for window, value in allocations:
            if point is None or window is None or window[0] is None or window[1] is None or \
                    window[0] <= point < window[1]:
                used = add(used, value)
        usages.append(used)
    return usages
