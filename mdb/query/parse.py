## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""parse -- parse path queries"""

from __future__ import absolute_import
import os, sys, functools, importlib
from ply import lex, yacc

__all__ = ('PathParser', 'path')


### Parser

def Parser(tokens, ast, optimize=True):
    """Make a path parser.

    The grammar is a generalized form of XPath 2.0.  Reading the XPath
    grammar <http://www.w3.org/TR/xpath20/#nt-bnf> is a good way to
    understand the overall design of this grammar.

    For the most part, productions are declared in the same order
    they're defined in the XPath grammar.  Departures from the XPath
    grammar are the use of a PLY precedence table for binary
    operations, instance-of/cast/treat have been dropped, KindTest is
    generalized, and Predicate is treated as a Step instead of as part
    of an Axis or Filter.

    For example, this is similar to what the PathParser constructor
    does:

        from mdb import parse

        (tokens, lexer) = parse.Lexer()
        path = parse.Parser(tokens, AST)
        path.parse("//.", lexer=lexer)

    """

    def p_Path(p):
        """Path : Expr"""
        p[0] = ast.Path(p[1])

    def p_Expr(p):
        """Expr : ExprList"""
        p[0] = ast.Expr(*p[1])

    def p_ExprList(p):
        """ExprList : ExprSingle"""
        p[0] = [p[1]]

    def p_ExprList_many(p):
        """ExprList : ExprList ',' ExprSingle"""
        p[0] = extend(p[1], p[3])

    def p_ExprSingle(p):
        """ExprSingle : ForExpr
                      | QuantifiedExpr
                      | IfExpr
                      | OrExpr"""
        p[0] = p[1]

    def p_ForExpr(p):
        """ForExpr : FOR VarInExpr RETURN ExprSingle"""
        p[0] = ast.For(p[2], p[4])

    def p_VarInExpr(p):
        """VarInExpr : VarRef IN ExprSingle"""
        p[0] = [ast.VarIn(p[1], p[3])]

    def p_VarInExpr_many(p):
        """VarInExpr : VarInExpr ',' VarRef IN ExprSingle"""
        p[0] = extend(p[1], ast.VarIn(p[3], p[5]))

    def p_QuantifiedExpr(p):
        """QuantifiedExpr : QUANTITY VarInExpr SATISFIES ExprSingle"""
        p[0] = ast.Quantified(p[1], p[2], p[4])

    def p_IfExpr(p):
        """IfExpr : IF '(' Expr ')' THEN ExprSingle ELSE ExprSingle"""
        p[0] = ast.If(p[3], p[6], p[8])

    def p_OrExpr(p):
        """OrExpr : AndExpr"""
        p[0] = p[1]

    def p_OrExpr_many(p):
        """OrExpr : OrExpr OR AndExpr"""
        p[0] = ast.Or(p[1], p[3])

    def p_AndExpr(p):
        """AndExpr : CmpExpr"""
        p[0] = p[1]

    def p_AndExpr_many(p):
        """AndExpr : AndExpr AND CmpExpr"""
        p[0] = ast.And(p[1], p[3])

    def p_CmpExpr(p):
        """CmpExpr : BinOpExpr"""
        p[0] = p[1]

    def p_CmpExpr_many(p):
        """CmpExpr : BinOpExpr CMP BinOpExpr"""
        p[0] = ast.CmpOp(p[2], p[1], p[3])

    precedence = (
        ('left', 'PLUS', 'MINUS'),
        ('left', 'STAR', 'DIV'),
        ('left', 'UNION'),
        ('left', 'INTERSECT'),
        ('right', 'UNARY')
    )

    def p_BinOpExpr(p):
        """BinOpExpr : ValueExpr
                     | UnaryExpr"""
        p[0] = p[1]

    def p_BinOpExpr_op(p):
        """BinOpExpr : BinOpExpr PLUS BinOpExpr
                     | BinOpExpr MINUS BinOpExpr
                     | BinOpExpr STAR BinOpExpr
                     | BinOpExpr DIV BinOpExpr
                     | BinOpExpr UNION BinOpExpr
                     | BinOpExpr INTERSECT BinOpExpr"""
        p[0] = ast.BinOp(p[2], p[1], p[3])

    def p_UnaryExpr(p):
        """UnaryExpr : PLUS ValueExpr %prec UNARY
                     | MINUS ValueExpr %prec UNARY"""
        p[0] = ast.UnaryOp(p[1], p[2])

    def p_ValueExpr(p):
        """ValueExpr : PathExpr"""
        p[0] = p[1]

    ## See <http://www.w3.org/TR/xpath20/#doc-xpath-PathExpr>
    ROOT = ast.Axis(ast.Name('self'), ast.ReduceAxis(ast.Name('root'), []))
    DESCENDANT = ast.Axis(ast.Name('descendant-or-self'), ast.Pattern('*'))

    def p_PathExpr_root(p):
        """PathExpr : '/'"""
        p[0] = ast.PathExpr(ROOT)

    def p_PathExpr_abs(p):
        """PathExpr : '/' RelativePathExpr"""
        p[0] = ast.PathExpr(ROOT, *p[2])

    def p_PathExpr_abs_dslash(p):
        """PathExpr : DSLASH RelativePathExpr"""
        p[0] = ast.PathExpr(ROOT, DESCENDANT, *p[2])

    def p_PathExpr_rel(p):
        """PathExpr : RelativePathExpr"""
        p[0] = ast.PathExpr(*p[1])

    def p_RelativePathExpr(p):
        """RelativePathExpr : StepExpr
                            | Predicate"""
        p[0] = [p[1]]

    def p_RelativePathExpr_slash(p):
        """RelativePathExpr : RelativePathExpr '/' StepExpr"""
        p[0] = extend(p[1], p[3])

    def p_RelativePathExpr_dslash(p):
        """RelativePathExpr : RelativePathExpr DSLASH StepExpr"""
        p[0] = extend(p[1], DESCENDANT, p[3])

    def p_RelativePathExpr_pred(p):
        """RelativePathExpr : RelativePathExpr Predicate"""
        p[0] = extend(p[1], p[2])

    def p_StepExpr(p):
        """StepExpr : FilterExpr
                    | AxisStep"""
        p[0] = p[1]

    def p_AxisStep(p):
        """AxisStep : QName DCOLON NodeReduce"""
        p[0] = ast.Axis(p[1], p[3])

    ## FIXME: leave this out for now; add back after:
    ##    (a) deciding whether 'self' is right
    ##    (b) deciding if '/::test()' is just too ugly.

    # def p_AxisStep_reduce(p):
    #     """AxisStep : DCOLON NodeReduce"""
    #     p[0] = ast.Axis(ast.Name('self'), p[2])

    def p_AxisStep_attr(p):
        """AxisStep : '@' NodeReduce"""
        p[0] = ast.Axis(ast.Name('attribute'), p[2])

    def p_AxisStep_parent(p):
        """AxisStep : DDOT"""
        p[0] = ast.Axis(ast.Name('parent'), ast.Pattern('*'))

    def p_AxisStep_child(p):
        """AxisStep : NodeTest"""
        p[0] = ast.Axis(ast.Name('child'), p[1])

    def p_NodeReduce(p):
        """NodeReduce : ReduceAxis
                      | NodeTest"""
        p[0] = p[1]

    def p_NodeTest(p):
        """NodeTest : NameTest
                    | Wildcard"""
        p[0] = p[1]

    def p_NameTest(p):
        """NameTest : QName"""
        p[0] = ast.NameTest(p[1])

    def p_Wildcard(p):
        """Wildcard : STAR"""
        p[0] = ast.Pattern(None, '*')

    def p_Wildcard_ns(p):
        """Wildcard : STAR ':' NAME
                    | NAME ':' STAR"""
        p[0] = ast.Pattern(p[1], p[3])

    def p_FilterExpr(p):
        """FilterExpr : PrimaryExpr"""
        p[0] = ast.Filter(p[1])

    def p_Predicate(p):
        """Predicate : '[' Expr ']'"""
        p[0] = ast.Predicate(p[2])

    def p_PrimaryExpr(p):
        """PrimaryExpr : Literal
                       | VarRef
                       | ParenExpr
                       | ContextItem
                       | FunctionCall"""
        p[0] = p[1]

    def p_Literal_num(p):
        """Literal : INTEGER
                   | DECIMAL"""
        p[0] = ast.Number(p[1])

    def p_Literal_string(p):
        """Literal : STRING"""
        p[0] = ast.String(p[1])

    def p_VarRef(p):
        """VarRef : '$' AnyName"""
        p[0] = ast.Ref(p[2])

    def p_ParenExpr(p):
        """ParenExpr : '(' Expr ')'"""
        p[0] = p[2]

    def p_ParenExpr_null(p):
        """ParenExpr : '(' ')'"""
        p[0] = ast.Expr()

    def p_ContextItem(p):
        """ContextItem : '.'"""
        p[0] = ast.ContextItem()

    def p_FunctionCall(p):
        """FunctionCall : QName '(' Arguments ')'"""
        p[0] = ast.Apply(p[1], p[3])

    def p_Arguments(p):
        """Arguments : Arguments ',' ExprSingle"""
        p[0] = extend(p[1], p[3])

    def p_Arguments_one(p):
        """Arguments : ExprSingle"""
        p[0] = [p[1]]

    def p_Arguments_none(p):
        """Arguments : """
        p[0] = []

    def p_ReduceAxis(p):
        """ReduceAxis : QName '(' Arguments ')'"""
        p[0] = ast.ReduceAxis(p[1], p[3])

    def p_AnyName(p):
        """AnyName : QName
                   | KeywordName"""
        p[0] = p[1]

    def p_QName(p):
        """QName : NAME"""
        p[0] = ast.Name(p[1])

    def p_QName_ns(p):
        """QName : NAME ':' NAME"""
        p[0] = ast.Name(p[1], p[3])

    def p_KeywordName(p):
        """KeywordName : RETURN
                       | FOR
                       | IN
                       | QUANTITY
                       | SATISFIES
                       | IF
                       | ELSE
                       | OR
                       | AND
                       | CMP
                       | TO
                       | DIV
                       | UNION
                       | INTERSECT"""
        p[0] = ast.Name(p[1])

    def p_error(p):
        raise BadToken(p)

    return yacc.yacc(
        tabmodule=_table('_parsetab', optimize),
        outputdir=os.path.dirname(__file__),
        optimize=optimize,
        debug=(not optimize)
    )

def extend(seq, *items):
    seq.extend(items)
    return seq

def _table(name, optimize):
    if not optimize:
        return name
    try:
        return importlib.import_module('..%s' % name, __name__)
    except ImportError:
        return name


### Lexer

def Lexer(optimize=True):
    """A Path lexer."""

    tokens = [
        'MINUS', 'STAR', 'PLUS', 'DSLASH', 'DCOLON', 'DDOT',
        'STRING', 'INTEGER', 'DECIMAL', 'NAME'
    ]

    reserved = {
        'return': 'RETURN',
        'for': 'FOR',
        'in': 'IN',
        'some': 'QUANTITY',
        'every': 'QUANTITY',
        'satisfies': 'SATISFIES',
        'if': 'IF',
        'then': 'THEN',
        'else': 'ELSE',
        'or': 'OR',
        'and': 'AND',
        'eq': 'CMP',
        'ne': 'CMP',
        'lt': 'CMP',
        'le': 'CMP',
        'gt': 'CMP',
        'ge': 'CMP',
        'is': 'CMP',
        'to': 'TO',
        'div': 'DIV',
        'mod': 'DIV',
        'union': 'UNION',
        'intersect': 'INTERSECT',
        'except': 'INTERSECT'
    }

    tokens.extend(set(reserved.itervalues()))

    literals = "[](),/.@:?$"

    t_CMP = r'!?=|<[=<]?|>[=>]?'
    t_PLUS = r'\+'
    t_MINUS = r'\-'
    t_STAR = r'\*'
    t_UNION = r'\|'
    t_DSLASH = r'//'
    t_DCOLON = r'::'
    t_DDOT = r'\.\.'

    def t_INTEGER(t):
        ur'\d+'
        t.value = int(t.value)
        return t

    def t_DECIMAL(t):
        ur'(?:\d+\.\d*|\.\d+)(?:[eE][\+\-]?\d+)?'
        t.value = float(t.value)
        return t

    def t_NAME(t):
        ur'[a-zA-Z][\w\-]*'
        t.type = reserved.get(t.value, 'NAME')
        return t

    def t_STRING(t):
        ur"""(?:"(?:[^"]|"")*"|'(?:[^']|'')*')"""
        val = t.value
        quote = val[0]
        t.value = val[1:-1].replace(quote * 2, quote)
        return t

    t_ignore = ' \t\n\r'

    def t_error(t):
        print 'Illegal character %r at %r' % (t.value[0], t.value[0:15])

    lexer = lex.lex(
        lextab=_table('_lextab', optimize),
        outputdir=os.path.dirname(__file__),
        optimize=optimize,
        debug=(not optimize)
    )
    return (tokens, lexer)


### AST

class AST(object):
    """This is an "identity" AST for the Path parser.  It simply
    captures the structure of the parsed expression."""

    def __init__(self, tag, *elements):
        self.tag = tag
        self.elem = elements

    def __repr__(self):
        return '<%s: %s>' % (self.tag, ' '.join(repr(e) for e in self.elem))

    def node(name):
        return classmethod(lambda c, *e: c(name, *e))

    And = node("And")
    Apply = node("Apply")
    Axis = node("Axis")
    BinOp = node("BinOp")
    CmpOp = node("CmpOp")
    ContextItem = node("ContextItem")
    Expr = node("Expr")
    Filter = node("Filter")
    For = node("For")
    If = node("If")
    Name = node("Name")
    NameTest = node("NameTest")
    Number = node("Number")
    Or = node("Or")
    Path = node("Path")
    PathExpr = node("PathExpr")
    Pattern = node("Pattern")
    Predicate = node("Predicate")
    ReduceAxis = node("ReduceAxis")
    Quantified = node("Quantified")
    Ref = node("Ref")
    String = node("String")
    UnaryOp = node("UnaryOp")
    VarIn = node("VarIn")

    del node


### Public Interface

class BadToken(Exception):
    """Raised from the p_error() handler in the parser."""

def parse(parser, lexer, data, debug=False):
    """Wrap parser.parse to produce a decent error message."""

    try:
        return parser.parse(data, lexer=lexer, debug=debug)
    except BadToken as exc:
        tok = exc.args[0]
        if not tok:
            raise SyntaxError('Unexpected end of statement.')

        context = data[max(0, tok.lexpos - 15):tok.lexpos + len(tok.value)]
        if tok.lexpos > 15:
            context = '...%s' % context
        raise SyntaxError('Unexpected %s %r at position %d (%r)' % (
            tok.type,
            tok.value,
            tok.lexpos,
            context
        ))

def PathParser(ast=AST, **kwargs):
    """Create a path parser using the given AST."""

    optimize = not kwargs.get('debug', False)
    (tokens, lexer) = Lexer(optimize=optimize)
    parser = Parser(tokens, ast, optimize=optimize)
    return functools.partial(parse, parser, lexer, **kwargs)

path = PathParser() # default parser
