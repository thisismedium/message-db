## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""query_ast -- extentions to the query abstract syntax tree"""

from __future__ import absolute_import
from ..query import ast as _ast
from ..query.ast import *

def NameTest(name):
    """Make the NameTest case-sensitive.  Lowercase tests for names
    like normal.  Uppercase names are assumed to be a model class
    name."""

    name = name.id
    if name[0].islower():
        return _ast.String(name)
    else:
        return _ast.Op('kind', _ast.String(name))
