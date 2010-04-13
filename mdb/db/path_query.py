## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

from __future__ import absolute_import
from ..query import compiler as comp, parse
from . import query_ast, query_ops

__all__ = ('compile', 'PathQuery')

def compile(expr):
    """Compile a path query.

    For example, this will compile a path query into and object that
    can be called against some context item:

       db.compile('//Page')(db.root())
    """

    return PathQuery(expr)


### Compile Path Queries

PathQuery = comp.Evaluator(
    parse.PathParser(query_ast),
    comp.builtin(comp.use(query_ops))
)
