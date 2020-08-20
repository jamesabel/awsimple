from math import isinf, isnan, nan, inf

from typeguard import typechecked

rel_tol_default = 1e-09
abs_tol_default = 0.0

# todo: put this in PyPI as it's own package.  Even though dictdiffer exists this is slightly different ...


class ValueDivergence:
    @typechecked(always=True)
    def __init__(self, label: (str, None), value):
        self.label = label
        self.value = value

    def __repr__(self):
        v = str(self.value)
        if self.label is None:
            s = v
        else:
            s = self.label + ":" + v
        return s

    def __eq__(self, other):
        return self.label == other.label and self.value == other.value

    def to_sort(self):
        if isinstance(self.value, float) or isinstance(self.value, int):
            return self.value
        else:
            return 0.0  # for strings, etc. just use 0.0 to sort


class ValueDivergences:
    @typechecked(always=True)
    def __init__(self, max_divergences: int = 10):
        self.max_divergences = max_divergences
        self.divergences = []
        self.hit_max_divergences_flag = False

    def __repr__(self):
        return self.divergences.__repr__()

    def __len__(self):
        return len(self.divergences)

    @typechecked(always=True)
    def add(self, divergence: ValueDivergence):

        if not any([d == divergence for d in self.divergences]):
            self.divergences.append(divergence)
            self.divergences.sort(key=lambda x: x.to_sort())
            if len(self.divergences) > self.max_divergences:
                self.divergences.pop()
                self.hit_max_divergences_flag = True

    def get(self):
        return self.divergences

    def max_value(self):
        mv = None
        if len(self.divergences) > 0:
            mv = self.divergences[-1].value
        if not (isinstance(mv, float) or isinstance(mv, int)):
            mv = 0.0
        return mv

    def max_label(self):
        ml = None
        if len(self.divergences) > 0:
            ml = self.divergences[-1].label
        return ml

    def hit_max_divergences(self):
        # is max incomplete?
        return self.hit_max_divergences_flag


class DictIsClose:
    """
    Like doing x == y for a dict, except if there are floats then use math.isclose()
    """

    @typechecked(always=True)
    def __init__(self, x, y, rel_tol: float = None, abs_tol: float = None, divergences: ValueDivergences = ValueDivergences()):
        self._x = x
        self._y = y
        self._rel_tol = rel_tol
        self._abs_tol = abs_tol
        self.divergences = divergences
        self._is_close_flag = self._dict_is_close(self._x, self._y, self._rel_tol, self._abs_tol, None)

    def __repr__(self):
        return self.divergences.__repr__()

    @typechecked(always=True)
    def _is_close_number(self, a: (float, int), b: (float, int), rel_tol: float, abs_tol: float, value_label: (str, None)):

        """
        similar to math.isclose() except is keeps track of which values have the greatest difference
        :param a: first input
        :param b: second input
        :param rel_tol: relative tolerance
        :param abs_tol: absolute tolerance
        :return:
        """

        # handle NaN, INF.  Matches math.isclose() .
        divergence_value = 0.0
        if isnan(a) or isnan(b):
            is_close_flag = False
            divergence_value = nan
        elif isinf(a) and isinf(b):
            is_close_flag = a == b  # handles both +INF and -INF
            if not is_close_flag:
                divergence_value = inf
        elif isinf(a) or isinf(b):
            is_close_flag = False  # only one or the other is (positive or negative) infinity
            divergence_value = inf
        elif isinf(rel_tol) or isinf(abs_tol):
            is_close_flag = True
        else:
            # is_close_flag is same as:
            # abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)
            divergence_value = abs(a - b) - max(rel_tol * max(abs(a), abs(b)), abs_tol)  # if > 0.0, values are *not* close
            is_close_flag = divergence_value <= 0.0

        if not is_close_flag and divergence_value is not None and (self.divergences.max_value() is None or divergence_value > self.divergences.max_value()):
            self.divergences.add(ValueDivergence(value_label, divergence_value))

        return is_close_flag

    @typechecked(always=True)
    def _dict_is_close(self, x, y, rel_tol: (float, None), abs_tol: (float, None), parent_label: (str, None)):

        if rel_tol is None or isnan(rel_tol):
            rel_tol = rel_tol_default
        if abs_tol is None:
            abs_tol = abs_tol_default

        if (isinstance(x, float) or isinstance(x, int)) and (isinstance(y, float) or isinstance(y, int)):
            is_close_flag = self._is_close_number(x, y, rel_tol, abs_tol, parent_label)
        elif isinstance(x, dict) and isinstance(y, dict):
            is_close_flags = []
            if set(x.keys()) == set(y.keys()):
                for k in x:

                    # keys can be things other than strings, e.g. int
                    str_k = str(k)
                    if parent_label is None:
                        label = str_k
                    else:
                        label = parent_label + "." + str_k

                    is_close_flag = self._dict_is_close(x[k], y[k], rel_tol, abs_tol, label)
                    is_close_flags.append(is_close_flag)
            is_close_flag = all(is_close_flags)
        else:
            is_close_flag = x == y  # everything else that can be evaluated with == such as strings
            if not is_close_flag:
                self.divergences.add(ValueDivergence(parent_label, str(x)))

        return is_close_flag

    def is_close(self):
        return self._is_close_flag


@typechecked(always=True)
def dict_is_close(x, y, rel_tol: float = None, abs_tol: float = None):
    """

    Like doing x == y for a dict, except if there are floats then use math.isclose()

    :param x: input x
    :param y: input y
    :param rel_tol: relative tolerance to pass to math.close
    :param abs_tol: absolute tolerance to pass to math.close
    :return: True if dictionaries match and float values are close

    """
    return DictIsClose(x, y, rel_tol, abs_tol).is_close()
