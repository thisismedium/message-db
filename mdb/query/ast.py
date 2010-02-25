## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""ast -- path query abstract syntax tree"""

from __future__ import absolute_import
import ast

__all__ = (
    'Path', 'Expr', 'PathExpr', 'Filter', 'Predicate', 'ContextItem', 'Axis',
    'ReduceAxis', 'NameTest', 'Pattern', 'For', 'VarIn', 'Quantified', 'If',
    'Apply', 'Ref', 'Name', 'Store', 'Number', 'String', 'And', 'Or',
    'UnaryOp', 'BinOp', 'CmpOp'
)

def Path(expr):
    return ast.Expression(Op('Path', expr))

def Expr(*expr):
    if len(expr) == 1:
        return expr[0] if is_simple(expr[0]) else Op('sequence', expr[0])
    return ast.Tuple([Op('sequence', e) for e in expr], ast.Load())

def PathExpr(*steps):
    if len(steps) == 1 and is_simple(steps[0]):
        return steps[0]
    return Op('steps', *steps)

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
    return ast.Str(name.id)

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


### Shortcuts

def identifier(names):
    return ':'.join(n for n in names if n)

def is_simple(obj):
    return isinstance(obj, ast.Num)

def Op(name, *args):
    return Apply(Name(name), list(args))

def Lambda(args, body):
    return ast.Lambda(ast.arguments(args, None, None, []), body)

def Thunk(expr):
    return Lambda([], expr)
