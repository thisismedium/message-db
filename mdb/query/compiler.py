## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""compiler -- compile path queries"""

from __future__ import absolute_import
import sys, __builtin__, ast as _ast
from . import parse, ops, tree, ast

__all__ = ('read', 'evaluate')


### Compiler

def Evaluator(parse, GLOBAL):
    def evaluate(code):
        if isinstance(code, basestring):
            code = compile_ast(parse(code))
        return eval(code, GLOBAL, {})
    return evaluate

def compile_ast(node, filename='<string>', mode='eval'):
    ## print _ast.dump(ast.fix_missing_locations(node))
    return compile(_ast.fix_missing_locations(node), filename, mode)


### Environment

def environment(*modules):
    result = {}
    for (env, names) in modules:
        for name in names:
            bind(result, name, env[name])
    return result

def bind(env, name, value):
    env[binding_name(name)] = value

def binding_name(name):
    ## The XPath convention is to hyphenate identifiers.  Translate
    ## from Python identifier conventions.
    return name.replace('_', '-')

def use(mod, *only):
    return (mod.__dict__, only or exported(mod))

def exported(mod):
    try:
        return mod.__all__
    except AttributeError:
        return (n for n in dir(mod) if not n.startswith('_'))

def builtin(*modules):
    return environment(
        use(__builtin__, 'list', 'int', 'float'),
        use(ops),
        use(tree),
        *modules
    )

class Gensym(object):
    __slots__ = ('prefix', 'index')

    def __init__(self, prefix):
        self.prefix = prefix
        self.index = -1

    def __call__(self, name=None):
        self.index += 1
        return '%s#%s' % (name or self.prefix, self.index)


### Defaults

gensym = Gensym('g')

read = parse.PathParser(ast)

evaluate = Evaluator(read, { '__builtins__' : builtin() })
