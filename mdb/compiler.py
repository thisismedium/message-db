## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""compiler -- compile path queries"""

from __future__ import absolute_import
import sys, ast, __builtin__
from . import parse, ops, tree, datastore

__all__ = ('read', 'evaluate')


### AST

def Path(expr):
    return ast.Expression(Op('XPath', expr))

def Expr(*expr):
    if len(expr) == 1:
        return expr[0] if is_simple(expr[0]) else Op('sequence', expr[0])
    return ast.Tuple([Op('sequence', e) for e in expr], ast.Load())

def PathExpr(*steps):
    if len(steps) == 1 and is_simple(steps[0]):
        return steps[0]
    return Op('path', *steps)

def Filter(primary):
    if is_simple(primary):
        return primary
    return Op('filter', Thunk(primary))

def Predicate(pred):
    return Op('predicate', pred if isinstance(pred, ast.Num) else Thunk(pred))

def ContextItem():
    return Op('focus')

def Axis(name, test):
    return Op(name.id, test)

def ReduceAxis(name, args):
    if not args:
        return name
    args.insert(gensym('items'))
    return Lambda(args, Apply(name, args))

def NameTest(name):
    name = name.id
    if name[0].islower():
        return ast.Str(name)
    else:
        return Op('kind', ast.Str(name))

def Pattern(*names):
    name = identifier(names)
    if name == '*':
        return ast.Name('None', ast.Load())
    raise NotImplementedError('The only valid pattern is "*".')

def For(var_in, body):
    gen = [ast.comprehension(Store(n.id), s, []) for (n, s) in var_in]
    ## Nested for-loops are elided together.
    if isinstance(body, ast.GeneratorExp):
        gen.extend(body.generators)
        body = body.elt
    return ast.GeneratorExp(body, gen)

def VarIn(name, body):
    return (name, body)

def Quantified(quant, var_in, body):
    return Op(quant, For(var_in, body))

def If(test, body, orelse):
    return ast.IfExp(test, body, orelse)

def Apply(name, args):
    return ast.Call(name, args, [], None, None)

## Names and Literals

def Ref(name):
    return name

def Name(*names):
    return ast.Name(identifier(names), ast.Load())

def Store(lname):
    return ast.Name(lname, ast.Store())

def Number(value):
    return ast.Num(value)

def String(value):
    return ast.Str(value)

def identifier(names):
    return ':'.join(n for n in names if n)

def is_simple(obj):
    return isinstance(obj, ast.Num)

## Operations

def And(*expr):
    return ast.BoolOp(ast.And(), list(expr))

def Or(*expr):
    return ast.BoolOp(ast.Or(), list(expr))

UNARY = { '+': ast.UAdd, '-': ast.USub }

def UnaryOp(op, value):
    if isinstance(value, ast.Num):
        value.n = (-1 if op == '-' else 1) * value.n
        return value
    return ast.UnaryOp(UNARY[op](), value)

BINOP = {
    '+': ast.Add,
    '-': ast.Sub,
    '*': ast.Mult,
    'mod': ast.Mod,
    'div': ast.Div,
}

def BinOp(op, left, right):
    op = BINOP[op]
    if isinstance(op, ast.Name):
        return Op(op, left, right)
    return ast.BinOp(left, op(), right)

CMPOP = {
    '<': ast.Lt,
    '<=': ast.LtE,
    '>': ast.Gt,
    '>=': ast.GtE,
    '=': ast.Eq,
    '!=': ast.NotEq,
    'is': ast.Is,
    'is-not': ast.IsNot,
    'in': ast.In,
    'not-in': ast.NotIn
}

def CmpOp(op, left, right):
    return ast.Compare(left, [CMPOP[op]()], [right])

def Op(name, *args):
    return Apply(Name(name), list(args))

def Lambda(args, body):
    return ast.Lambda(ast.arguments(args, None, None, []), body)

def Thunk(expr):
    return Lambda([], expr)


### Compiler

def evaluator(parse, GLOBAL):
    def evaluate(code):
        if isinstance(code, basestring):
            code = compile_ast(parse(code))
        return eval(code, GLOBAL, {})
    return evaluate

def compile_ast(node, filename='<string>', mode='eval'):
    ## print ast.dump(ast.fix_missing_locations(node))
    return compile(ast.fix_missing_locations(node), filename, mode)


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

class Gensym(object):
    __slots__ = ('prefix', 'index')

    def __init__(self, prefix):
        self.prefix = prefix
        self.index = -1

    def __call__(self, name=None):
        self.index += 1
        return '%s#%s' % (name or self.prefix, self.index)

gensym = Gensym('g')


### Defaults

read = parse.PathParser(sys.modules[__name__])

evaluate = evaluator(read, {
    '__builtins__' : environment(
        use(__builtin__, 'list', 'int', 'float'),
        use(ops),
        use(tree),
    )
})
