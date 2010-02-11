## Copyright (c) 2010, Coptix, Inc.  All rights reserved.
## See the LICENSE file for license terms and warranty disclaimer.

"""parse -- parse path queries"""

from __future__ import absolute_import
import sys, functools
from ply import lex, yacc

__all__ = ('PathParser', 'path')


### Lexer

def lex_path():
    """An XPath lexer; see parse_path() below."""

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
        'idiv': 'DIV',
        'mod': 'DIV',
        'union': 'UNION',
        'intersect': 'INTERSECT',
        'except': 'INTERSECT',
        'of': 'OF',
        'instance': 'INSTANCE',
        'as': 'AS',
        'treat': 'TREAT',
        'castable': 'CASTABLE',
        'cast': 'CAST',
        'child': 'AXIS',
        'descendant': 'AXIS',
        'attribute': 'AXIS',
        'self': 'AXIS',
        'descendant-or-self': 'AXIS',
        'following-sibling': 'AXIS',
        'following': 'AXIS',
        'namespace': 'AXIS',
        'parent': 'AXIS',
        'ancestor': 'AXIS',
        'preceeding-sibling': 'AXIS',
        'preceeding': 'AXIS',
        'ancestor-or-self': 'AXIS',
        'empty-sequence': 'EMPTYSEQ',
        'item': 'ITEM',
        'node': 'KIND',
        'text': 'KIND',
        'comment': 'KIND',
        'document-node': 'DOCNODE',
        'processing-instructions': 'PI',
        'attribute': 'ATTR',
        'schema-attribute': 'SCHEMA',
        'element': 'ELEM',
        'schema-element': 'SCHEMA'
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

    return (tokens, lex.lex())


### Parser

def parse_path(tokens, ast):
    """Make an XPath 2.0 parser <http://www.w3.org/TR/xpath20/#nt-bnf>

    Reading the XPath grammar linked above is the best way to
    understand this parser.

    For the most part, productions are declared in the same order
    they're defined in the XPath grammar.  Notable departures are the
    use of a PLY precedence table for binary operations and some QName
    productions (e.g. ElementDeclaration, AttributeName, etc) have
    been elided together.
    """

    def p_XPath(p):
        """XPath : Expr"""
        p[0] = p[1]

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
        """ForExpr : SimpleForClause RETURN ExprSingle"""
        p[0] = ast.For(p[1], p[2])

    def p_SimpleForClause(p):
        """SimpleForClause : FOR VarInExpr"""
        p[0] = p[2]

    def p_VarInExpr(p):
        """VarInExpr : VarRef IN ExprSingle"""
        p[0] = [ast.VarIn(p[1], p[3])]

    def p_VarInExpr_many(p):
        """VarInExpr : VarInExpr ',' VarRef IN ExprSingle"""
        p[0] = extend(p[1], ast.VarIn(p[3], p[5]))

    def p_QuantifiedExpr(p):
        """QuantifiedExpr : QUANTITY VarInExpr SATISFIES ExprSingle"""
        p[0] = ast.Quantified([1], p[2], p[3])

    def p_IfExpr(p):
        """IfExpr : IF '(' Expr ')' THEN ExprSingle ELSE ExprSingle"""
        p[0] = Ast.If(p[3], p[6], p[8])

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
        """CmpExpr : RangeExpr"""
        p[0] = p[1]

    def p_CmpExpr_many(p):
        """CmpExpr : RangeExpr CMP RangeExpr"""
        p[0] = ast.CmpOp(p[2], p[1], p[3])

    def p_RangeExpr(p):
        """RangeExpr : BinOpExpr"""
        p[0] = p[1]

    def p_RangeExpr_many(p):
        """RangeExpr : BinOpExpr TO BinOpExpr"""
        p[0] = ast.Range(p[1], p[3])

    precedence = (
        ('left', 'PLUS', 'MINUS'),
        ('left', 'STAR', 'DIV'),
        ('left', 'UNION'),
        ('left', 'INTERSECT'),
        ('left', 'INSTANCE'),
        ('left', 'TREAT'),
        ('left', 'CASTABLE'),
        ('left', 'CAST'),
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

    def p_BinOpExpr_phrase(p):
        """BinOpExpr : BinOpExpr INSTANCE OF SequenceType
                     | BinOpExpr TREAT AS SequenceType
                     | BinOpExpr CASTABLE AS SingleType
                     | BinOpExpr CAST AS SingleType"""
        p[0] = ast.BinOp(p[2], p[1], p[4])

    def p_UnaryExpr(p):
        """UnaryExpr : PLUS ValueExpr %prec UNARY
                     | MINUS ValueExpr %prec UNARY"""
        p[0] = ast.UnaryOp(p[1], p[2])

    def p_ValueExpr(p):
        """ValueExpr : PathExpr"""
        p[0] = p[1]

    def make_path_abbr(proc, axis, test):
        ## 'axis:test()'
        result = ast.Axis(axis, ast.Test(test), [])
        if proc:
            ## 'proc(axis::test())'
            result = ast.Filter(
                ast.Apply(ast.Name(None, proc), [ast.Path(result)]),
                [])
        return result

    ## See <http://www.w3.org/TR/xpath20/#doc-xpath-PathExpr>
    ROOT = make_path_abbr('root', 'self', 'node')
    DESCENDANT = make_path_abbr(None, 'descendant-or-self', 'node')

    def p_PathExpr_root(p):
        """PathExpr : '/'"""
        p[0] = ast.Path(ROOT)

    def p_PathExpr_abs(p):
        """PathExpr : '/' RelativePathExpr"""
        p[0] = ast.Path(ROOT, *p[2])

    def p_PathExpr_abs_dslash(p):
        """PathExpr : DSLASH RelativePathExpr"""
        p[0] = ast.Path(ROOT, DESCENDANT, *p[2])

    def p_PathExpr_rel(p):
        """PathExpr : RelativePathExpr"""
        p[0] = ast.Path(*p[1])

    def p_RelativePathExpr(p):
        """RelativePathExpr : StepExpr"""
        p[0] = [p[1]]

    def p_RelativePathExpr_slash(p):
        """RelativePathExpr : RelativePathExpr '/' StepExpr"""
        p[0] = extend(p[1], p[3])

    def p_RelativePathExpr_dslash(p):
        """RelativePathExpr : RelativePathExpr DSLASH StepExpr"""
        p[0] = extend(p[1], DESCENDANT, p[3])

    def p_StepExpr(p):
        """StepExpr : FilterExpr
                    | AxisStep"""
        p[0] = p[1]

    def p_AxisStep(p):
        """AxisStep : AXIS DCOLON NodeTest PredicateList"""
        p[0] = ast.Axis(p[1], p[3], p[4])

    def p_AxisStep_attr(p):
        """AxisStep : '@' NodeTest PredicateList"""
        p[0] = ast.Axis('attribute', p[2], p[3])

    def p_AxisStep_parent(p):
        """AxisStep : DDOT PredicateList"""
        p[0] = ast.Axis('parent', ast.Name(None, '*'), p[2])

    def p_AxisStep_self(p):
        """AxisStep : NodeTest PredicateList"""
        p[0] = ast.Axis('self', p[1], p[2])

    def p_NodeTest(p):
        """NodeTest : KindTest
                    | NameTest"""
        p[0] = p[1]

    def p_NameTest(p):
        """NameTest : QName
                    | Wildcard"""
        p[0] = p[1]

    def p_Wildcard(p):
        """Wildcard : STAR"""
        p[0] = ast.Name(None, '*')

    def p_Wildcard_ns(p):
        """Wildcard : STAR ':' NAME
                    | NAME ':' STAR"""
        p[0] = ast.Name(p[1], p[3])

    def p_FilterExpr(p):
        """FilterExpr : PrimaryExpr PredicateList"""
        p[0] = ast.Filter(p[1], p[2])

    def p_PredicateList(p):
        """PredicateList : PredicateList Predicate"""
        p[0] = extend(p[1], p[2])

    def p_PredicateList_one(p):
        """PredicateList : Predicate"""
        p[0] = [p[1]]

    def p_PredicateList_empty(p):
        """PredicateList : """
        p[0] = []

    def p_Predicate(p):
        """Predicate : '[' Expr ']'"""
        p[0] = p[2]

    def p_PrimaryExpr(p):
        """PrimaryExpr : Literal
                       | VarRef
                       | ParenExpr
                       | ContextItem
                       | FunctionCall"""
        p[0] = p[1]

    def p_Literal(p):
        """Literal : INTEGER
                   | DECIMAL
                   | STRING"""
        p[0] = p[1]

    def p_VarRef(p):
        """VarRef : '$' AnyName"""
        p[0] = ast.Ref(p[2])

    def p_ParenExpr(p):
        """ParenExpr : '(' Expr ')'"""
        p[0] = p[1]

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

    def p_SingleType(p):
        """SingleType : AtomicType"""
        p[0] = ast.Type(p[1], False)

    def p_SingleType_maybe(p):
        """SingleType : AtomicType '?'"""
        p[0] = ast.Type(p[1], True)

    def p_SequenceType(p):
        """SequenceType : ItemType OccuranceIndicator"""
        p[0] = ast.SeqType(p[1], p[2])

    def p_SequenceType_one(p):
        """SequenceType : ItemType"""
        p[0] = ast.SeqType(p[1], None)

    def p_SequenceType_empty(p):
        """SequenceType : EMPTYSEQ '(' ')'"""
        p[0] = ast.SeqType(ast.Test(p[1]), None)

    def p_OccuranceIndicator(p):
        """OccuranceIndicator : '?'
                              | STAR
                              | PLUS"""
        p[0] = p[1]

    def p_ItemType(p):
        """ItemType : KindTest
                    | AtomicType"""
        p[0] = p[1]

    def p_ItemType_item(p):
        """ItemType : ITEM '(' ')'"""
        p[0] = mode.Test(p[1])

    def p_AtomicType(p):
        """AtomicType : QName"""
        p[0] = p[1]

    def p_KindTest(p):
        """KindTest : DocumentTest
                    | ElementTest
                    | AttributeTest
                    | SchemaTest
                    | PITest
                    | SimpleKindTest"""
        p[0] = p[1]

    def p_DocumentTest(p):
        """DocumentTest : DOCNODE '(' ElementTest ')'
                        | DOCNODE '(' SchemaTest ')'"""
        p[0] = ast.Test(p[1], p[3])

    def p_PITest(p):
        """PITest : PI '(' AnyName ')'
                  | PI '(' STRING ')'"""
        p[0] = ast.Test(p[1], p[3])

    def p_AttributeTest(p):
        """AttributeTest : ATTR '(' NameOrWildcard ')'"""
        p[0]= ast.Test(p[1], p[3])

    def p_AttributeTest_typed(p):
        """AttributeTest : ATTR '(' NameOrWildcard ',' AtomicType ')'"""
        p[0] = ast.Test(p[1], p[3], ast.Type(p[5], False))

    def p_NameOrWildcard(p):
        """NameOrWildcard : AnyName
                          | STAR"""
        p[0] = p[1]

    def p_SchemaTest(p):
        """SchemaTest : SCHEMA '(' AnyName ')'"""
        p[0] = ast.Test(p[1], p[3])

    def p_ElementTest(p):
        """ElementTest : ELEM '(' NameOrWildcard ')'"""
        p[0] = ast.Test(p[1], p[3])

    def p_ElementTest_typed(p):
        """ElementTest : ELEM '(' NameOrWildcard ',' SingleType ')'"""
        p[0] = ast.Test(p[1], p[3], p[5])

    def p_SimpleKindTest(p):
        """SimpleKindTest : KIND '(' ')'
                          | DOCNODE '(' ')'
                          | PI '(' ')'
                          | ATTR '(' ')'
                          | ELEM '(' ')'"""
        p[0] = ast.Test(p[1])

    def p_AnyName(p):
        """AnyName : QName
                   | KeywordName"""
        p[0] = p[1]

    def p_QName(p):
        """QName : NAME"""
        p[0] = ast.Name(None, p[1])

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
                       | INTERSECT
                       | OF
                       | INSTANCE
                       | AS
                       | TREAT
                       | CASTABLE
                       | CAST
                       | AXIS
                       | EMPTYSEQ
                       | ITEM
                       | KIND
                       | DOCNODE
                       | PI
                       | ATTR
                       | SCHEMA
                       | ELEM"""
        p[0] = ast.Name(None, p[1])

    def p_error(p):
        raise BadToken(p)

    return yacc.yacc()

def extend(seq, *items):
    seq.extend(items)
    return seq


### AST

class AST(object):

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
    Name = node("Name")
    Or = node("Or")
    Path = node("Path")
    Quantified = node("Quantified")
    Range = node("Range")
    Ref = node("Ref")
    SeqType = node("SeqType")
    Test = node("Test")
    Type = node("Type")
    UnaryOp = node("UnaryOp")
    VarIn = node("VarIn")

    del node


### Public Interface

class BadToken(Exception):
    """Raised from the p_error() handler in the parser."""

def parse(parser, lexer, data):
    """Wrap parser.parse to produce a decent error message."""
    try:
        return parser.parse(data, lexer=lexer)
    except BadToken as exc:
        tok = exc.args[0]
        context = data[max(0, tok.lexpos - 15):tok.lexpos + len(tok.value)]
        if tok.lexpos > 15:
            context = '...%s' % context
        raise SyntaxError('Unexpected %s %r at position %d (%r)' % (
            tok.type,
            tok.value,
            tok.lexpos,
            context
        ))

def PathParser(ast=AST):
    (tokens, lexer) = lex_path()
    parser = parse_path(tokens, ast)
    return functools.partial(parse, parser, lexer)

path = PathParser()
