#!/usr/bin/python
#
# HiPy - A Simple Python framework for Apache Hive
#
# Copyright 2011 Netflix, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#
# See http://code.google.com/a/apache-extras.org/p/hipy/ for documentation
#
# Version 1.1
#

from __future__ import with_statement           # Not needed for Python 2.6+

import sys, os, inspect, struct, zlib, datetime, shutil, subprocess, glob, tempfile, shutil, fileinput, fnmatch
from copy import copy
import simplejson as json
import pickle
from optparse import OptionParser

HiPyPath = __file__

#########################################
# Utility functions 
#########################################

def PopIfList( tuple ) :
    return tuple[0] if ( len( tuple ) == 1 and isinstance( tuple[0], list ) ) else tuple
    
def Bracket( string ) :
    if string[0] == '(' and string[len(string)-1] == ')' :
        b = 0
        n = 0
        for i in range(0,len(string)) :
            c=string[i]
            if c == '(' : b += 1
            if c == ')' : b -= 1
            if b <= 0 : n += 1
        
        if n == 1 :
            return string
        
    return '(' + string + ')'

# Stringify an object, ensuring there are quotes if it is in fact a string  
def QuotedString( arg ) :
    if not isinstance( arg, str ) :
        return str( arg )
    
    if len( arg ) > 1 :
        first = arg[0]
        last = arg[len(arg)-1]
        if ( first == last ) and ( first == '"' or first == "'" ) :
            arg = arg[1:len(arg)-1]
    return "'" + arg + "'"

#########################################
# Exceptions 
#########################################

class HiPyException(Exception) :
    def __init__( self, args ) :
        Exception.__init__( self, args )

#########################################
# Hive Types 
#########################################

class HiveType :
    def __init__( self, name, alt = None, var = None ) :
        self.name = name
        self.alt = name if alt is None else alt
        self.var = var
        
    def __str__( self ) :
        return self.name
        
    def __repr__( self ) :
        return 'HiPy.'+self.var if self.var else object.__repr__( self )
        
    def __eq__( self, other ) :
        return self.var == other.var
        
    def __ne__( self, other ) :
        return not ( self == other )

HiveTinyInt = HiveType( "TINYINT", var='HiveTinyInt' )
HiveSmallInt = HiveType( "SMALLINT", var='HiveSmallInt' )
HiveInt = HiveType( "INT", var='HiveInt' )
HiveBigInt = HiveType( "BIGINT", var='HiveBigInt' )
HiveBoolean = HiveType( "BOOLEAN", var='HiveBoolean' )
HiveFloat = HiveType( "FLOAT", var='HiveFloat' )
HiveDouble = HiveType( "DOUBLE", var='HiveDouble' )
HiveString = HiveType( "STRING", var='HiveString' )
HiveJson = HiveType( "STRING", "JSON", var='HiveJson' )

class HiveArray( HiveType ) :
    def __init__( self, datatype ) :
        HiveType.__init__( self, "ARRAY<" + datatype.name + ">" )
        self.datatype = datatype
        
    def __repr__( self ) :
        return 'HiPy.HiveArray( ' + repr( self.datatype ) + ' )'
        
    def __eq__( self, other ) :
        return isinstance( other, HiveArray ) and ( self.datatype == other.datatype )
        
class HiveMap( HiveType ) :
    def __init__( self, keytype, datatype ) :
        HiveType.__init__( self, "MAP<" + str( keytype )+ "," + str( datatype ) + ">" )
        self.keytype = keytype
        self.datatype = datatype
        
    def __repr__( self ) :
        return 'HiPy.HiveMap( ' + repr( self.keytype ) + ', ' + repr( self.datatype ) + ' )'
        
    def __eq__( self, other ) :
        return isinstance( other, HiveMap ) and self.keytype == other.keytype and self.datatype == other.datatype
        
class HiveStruct( HiveType ) :
    def __init__( self, schema ) :
        HiveType.__init__( self, "STRUCT<" + ",".join( [ key + ':' + str( type ) for ( key, type ) in schema ] ) + ">" )
        self.schema = schema
        
    def __repr__( self ) :
        return 'HiPy.HiveStruct( ' + repr( self.schema ) + ' )'
#       return '[' + ','.join( [ '("' + key + '",' + repr( type )+ ')' for ( key, type ) in schema ] ) + ']'

    def __eq__( self, other ) :
        return isinstance( other, HiveStruct ) and self.schema == other.schema
        

HiveBasicTypes = {  'TINYINT'   : HiveTinyInt,
                    'SMALLINT'  : HiveSmallInt,
                    'INT'       : HiveInt,
                    'BIGINT'    : HiveBigInt,
                    'BOOLEAN'   : HiveBoolean,
                    'FLOAT'     : HiveFloat,
                    'DOUBLE'    : HiveDouble,
                    'STRING'    : HiveString,
                    'JSON'      : HiveJson }
                    
def HiveTypeFromString( string ) :
    string = string.strip()
    ustring = string.upper()
    if ustring in HiveBasicTypes :
        return HiveBasicTypes[ ustring ]
    
    if ustring.startswith( "ARRAY<" ) and ustring.endswith( ">" ) :
        return HiveArray( HiveTypeFromString( string[ 6 : len( string ) - 1 ] ) )
        
    if ustring.startswith( "MAP<" ) and ustring.endswith( ">" ) :
        types = string[ 4 : len( string ) - 1 ].split( ',' )
        return HiveMap( HiveTypeFromString( types[0] ), HiveTypeFromString( types[1] ) )
    
    if ustring.startswith( "STRUCT<" ) and ustring.endswith( ">" ) :
        elements = string[ 7 : len( string ) - 1 ].split( ',' )
        schema = [ ]
        for element in elements :
            ( key, type ) = element.split( ':' )
            schema.append( ( key, HiveTypeFromString( type ) ) )
        return HiveStruct( schema )
        
    raise HiPyException( "Unrecognised Hive type: " + string )

def SchemaToSql( schema ) :
    return "( " + ", ".join( [ col.name + ' ' + str( col.type ) for col in schema ] ) + " ) "
    
def SchemaToPython( schema ) :
    return [ ( col.name, col.type ) for col in schema ]
    
def SchemaFromDescribe( describe ) :
    schema = [ ]
    for line in describe :
        elements = line.split( '\t' )
        key = elements[ 0 ]
        type = elements[ 1 ]
        schema.append( ( key, HiveTypeFromString( type ) ) )
    return schema

# Python representation of Hive types:
#   String gives basic type name
#   1-tuple indicates Hive array
#   2-tuple indicates Hive map
#   List is list of 2-tuples giving hive struct

#def PythonToHiveType( python ) :
#   if isinstance( python, str ) :
#       return HiveBasicTypes[ python ]
#   elif isinstance( python, tuple ) and len( python ) == 1 :
#       return HiveArray( PythonToHiveType( python[0] ) )
#   elif isinstance( python, tuple ) :
#       return HiveMap( PythonToHiveType( python[0] ), PythonToHiveType( python[1] ) )
#   elif isinstance( python, array ) :
#       return HiveStruct( [ ( key, PythonToHiveType( type ) ) for ( key, type ) in python ] )
        
def HiveTypeFromExpression( obj ) :
    if isinstance( obj, int ) : return HiveInt
    elif isinstance( obj, long ) : return HiveBigInt
    elif isinstance( obj, float ) : return HiveDouble
    elif isinstance( obj, bool ) : return HiveBoolean
    elif isinstance( obj, str ) : return HiveString
    
    return obj.type

#def PythonToSchema( pythonSchema ) :   
#   return [ Column( name, PythonToHiveType( type ) ) for ( name, type ) in pythonSchema ]

#########################################
# Columns
#
# A Column represents a data element with a type and optionally, a table to which it belongs and a name in that table
#       XXX What does it mean to have a name and no table ? The name is a convenient hint for when its added to a table
#
# A Column can be
#   - a source column in a Hive data table - uses the Column base type and has name and table defined
#   - an entry in a Select, referring to a column in another table - uses the As subclass to carry the reference to the original column
#   - an expression combining multiple other Columns and other information. There are several kinds
#       - a Cast expression, changing the type of a column - uses the Cast subclass
#       - an Arithmetic expression - currently converted to a string when first encountered (FIXME)
#       - a Function expression - uses the Function subclass
#       - a map expression - uses the MapEntry subclass
#
# Columns can appear in the following places
#   - In SELECT lists as "<full definition> AS <local name>"
#   - In WHERE clauses as "<full definition>"
#   - In SORT BY, DISTRIBUTE BY, CLUSTER BY as "<qualified name>"
#   - In GROUP BY as "<qualified name>"
#   - In ON clauses as "<qualified name>"
#   - In a schema as "<local name> <type>"
#
# <local name> refers to the name of the column in the current Select
# <qualified name> equals the <local name> when the column is in the current Select, or <table>.<local name> otherwise
# <full definition> is the full definition of the column
#
# str( column ) is shorthand for the full definition
#########################################

class Column :
    def __init__( self, name, type ) :
        self.table = None
        self.name = name
        self.type = type
    
    # Returns the reference to the column as it appears in SORT BY, DISTRIBUTE BY, CLUSTER BY and other SELECTs
    def __str__( self ) :
        return self.Expression()
    
    # Returns the (qualified) name of the column as it should appear in WHERE, SORT BY, DISTRIBUTE BY, CLUSTER BY and other SELECTs
    def QualifiedName( self ) :
        if not self.table or not self.table.name or self.table.inscope :
            return self.name
        return self.table.name + "." + self.name
        
    # Returns the expression for the column as it appears in WHERE and GROUP BY
    def Expression( self ) :
        return self.QualifiedName()
        
    def Table( self ) :
        return self.table
        
    def __getitem__( self, key ) :
        if isinstance( self.type, HiveMap ) and self.type.keytype == HiveString :
            return MapEntry( self.type.datatype, self, key )
        elif isinstance( self.type, HiveArray ) :
            return ArrayEntry( self.type.datatype, self, key )
            
        raise HiPyException( self.name + " is not a Hive Map type with string keys or Hive Array." )
        
    
    def __lt__( self, other ) : return Operator( '<', HiveBoolean, self, other )
    def __le__( self, other ) : return Operator( '<=', HiveBoolean, self, other )
    def __eq__( self, other ) : return Operator( '=', HiveBoolean, self, other ) if ( other != None ) else UnaryOperator( ' is null', HiveBoolean, self, True )
    def __ne__( self, other ) : return Operator( '!=', HiveBoolean, self, other ) if ( other != None ) else UnaryOperator( ' is not null', HiveBoolean, self, True )
    def __gt__( self, other ) : return Operator( '>', HiveBoolean, self, other )
    def __ge__( self, other ) : return Operator( '>=', HiveBoolean, self, other )

    def __add__( self, other ) : return Operator( '+', self.type, self, other )
    def __sub__( self, other ) : return Operator( '-', self.type, self, other ) 
    def __mul__( self, other ) : return Operator( '*', self.type, self, other )
    def __div__( self, other ) : return Operator( '/', self.type, self, other )
    def __mod__( self, other ) : return Operator( '%', self.type, self, other )
    
    def __and__( self, other ) : return Condition( 'AND', self, other )
    def __rand_( self, other ) : return Condition( 'AND', other, self )
    def __or__( self, other ) : return Condition( 'OR', self, other )
    def __ror__( self, other ) : return Condition( 'OR', other, self )
    
    def __invert__( self ) : return Not( self )

# Convert a schema ( list of ( name, type ) pairs ) to a list of Column objects 
def Columns( *schema ) :
    schema = PopIfList( schema )
    if ( len( schema ) > 0 ) :
        if not isinstance( schema[0], Column ) : schema =  [ Column( name, type ) for ( name, type ) in schema ]
    return schema
    
class UnaryOperator(Column) :
    def __init__( self, op, type, arg, postfix = False ) :
        Column.__init__( self, None, type )
        self.op = op
        self.arg = arg
        self.postfix = postfix
        
    def Table( self ) :
        return self.arg.Table() if isinstance( self.arg, Column ) else None
        
    def Expression( self ) :
        if self.postfix :
            return '(' + str( self.arg ) + self.op + ')'
        else :
            return '(' + self.op + str( self.arg ) + ')'

def Not( arg ) :
    return UnaryOperator( ' NOT ', HiveBoolean, arg )

class Operator(Column) :
    def __init__( self, op, type, lhs, rhs ) :
        Column.__init__( self, None, type )
        self.op = op
        self.lhs = lhs
        self.rhs = rhs
    
    def Table( self ) :
        if isinstance( self.lhs, Column ) :
            if isinstance( self.rhs, Column ) :
                if self.lhs.Table() == self.rhs.Table() :
                    return self.lhs.Table()
                else :
                    return False
            else :
                return self.lhs.Table()
        else :
            if isinstance( self.rhs, Column ) :
                return self.rhs.Table()
            else :
                return None
        
    def Expression( self ) :
        return '(' + QuotedString( self.lhs ) + self.op + QuotedString( self.rhs ) + ')'

def Like( lhs, rhs ) :
    return Operator( " LIKE ", HiveBoolean, lhs, rhs )
        
def Rlike( lhs, rhs ) :
    return Operator( " RLIKE ", HiveBoolean, lhs, rhs )
    
def Any( iterable ) :
    return reduce( lambda a, b: a | b, iterable )
    
def All( iterable ) :
    return reduce( lambda a, b : a & b, iterable )
        
class Condition() :
    def __init__( self, condition, lhs, rhs ) :
        self.condition = condition
        self.lhs = lhs
        self.rhs = rhs
        
    def __str__( self ) :
        return self.Expression()
        
    def Expression( self ) :
        return '(' + str( self.lhs ) + ' ' + self.condition + ' ' + str( self.rhs ) + ')'

class ArrayEntry(Column) :
    def __init__( self, type, array, index, name = None ) :
        Column.__init__( self, name if name else (array.name+str( index )), type )
        self.array = array
        self.index = index
        
    def Table( self ) :
        return self.array.Table()
        
    def Expression( self ) :
        return str( self.array ) + "[" + str( self.index ) + "]"

class MapEntry(Column) :
    def __init__( self, type, map, key, name = None ) :
        Column.__init__( self, name if name else key, type )
        self.map = map
        self.key = key
        
    def Table( self ) :
        return self.map.Table()
        
    def Expression( self ) :
        return str( self.map ) + "['" + str( self.key ) + "']"


#########################################
# Hive functions
#########################################

class As(Column) :
    def __init__( self, column, alias ) :
        Column.__init__( self, alias, column.type )
        self.original = column
    
    # This function is used only in SELECT and TRANSFORM for the definition of a column
    def Define( self, alias = True ) :
        expression = self.original.QualifiedName() if self.original.table else self.original.Expression()
        if ( not alias ) or ( self.original.table and self.original.name == self.name ) or ( isinstance( self.original, MapEntry ) and self.original.key == self.name ) :
            return expression
        return expression + ' AS ' + self.name
    
    def Expression( self ) :
        if self.table :
            return self.QualifiedName()
        if self.original.table :
            return self.original.QualifiedName()
        return self.original.Expression()
        
class Function(Column) :
    def __init__( self, type, function, name, *parameters ) :
        Column.__init__( self, name, type )
        self.function = function
        self.parameters = PopIfList( parameters )
        
    def Table( self ) :
        tables = frozenset( [ parameter.Table() for parameter in self.parameters if isinstance( parameter, Column ) ] )
        if len( tables ) == 1 :
            [ table ] = tables
            return table
        return None
        
    def Expression( self ) :
        sql = self.function
        if self.parameters and len( self.parameters ) > 0 :
            sql = sql + "( " + " ,".join( [ str( parameter ) for parameter in self.parameters ] ) + " )"
        return sql

class Cast(Column) :
    def __init__( self, expression, type, name = None ) :
        Column.__init__( self, name if name else expression.name, type )
        self.expression = expression
    
    def Table( self ) :
        return self.expression.Table()
    
    def Expression( self ) :
        return "CAST( " + str( self.expression ) + " AS " + str( self.type ) + " )" 
        
class Max(Function) :
    def __init__( self, expression, name = None ) :
        Function.__init__( self, HiveTypeFromExpression( expression ), "MAX", name, expression )

class Sum(Function) :
    def __init__( self, expression, name = None ) :
        Function.__init__( self, HiveTypeFromExpression( expression ), "SUM", name, expression )
        
class Avg(Function) :
    def __init__( self, expression, name = None ) :
        Function.__init__( self, HiveTypeFromExpression( expression ), "AVG", name, expression )
        
class If(Function) :
    def __init__( self, condition, iftrue, iffalse, name = None ) :
        Function.__init__( self, HiveTypeFromExpression( iftrue ), "IF", name, condition, iftrue, iffalse )
        
class Count(Function) :
    def __init__( self, expression = None, name = None ) :
        Function.__init__( self, HiveInt, "COUNT", name, expression if expression else '1' )

class Distinct(Function) :
        def __init__( self, expression = None, name = None ) :
                Function.__init__( self, HiveTypeFromExpression(expression), "DISTINCT", name, expression)
        
class CountExpr(Count) :
    def __init__( self, expression, name = None ) :
        Count.__init__( self, If( expression, 'true', 'null' ), name )
        
class PercentileBase(Function) :
    def __init__( self, function, expression, percentiles, name = None ) :
        try :
            percentiles = "ARRAY(" + ','.join( [ "%0.7f" % x for x in percentiles ] ) + ")"
        except :
            pass
        Function.__init__( self, HiveArray( HiveDouble ), function, name, expression, percentiles )
                                    
class Percentile(PercentileBase) :
    def __init__( self, expression, percentiles, name = None ) :
        PercentileBase.__init__( self, "PERCENTILE", expression, percentiles, name )
        
class PercentileApprox(PercentileBase) :
    def __init__( self, expression, percentiles, name = None ) :
        PercentileBase.__init__( self, "PERCENTILE_APPROX", expression, percentiles, name )

class Cdf(Percentile) :
    _p = [ 0.5 * pow( 0.8, i ) for i in range( 57, 1, -1 ) ]
    Percentiles = _p + [ 0.5 ] + [ 1-x for x in reversed( _p ) ]
    
    def __init__( self, expression, name = None ) :
        Percentile.__init__( self, expression, Cdf.Percentiles, name )


#########################################
# SQL Construction
#########################################

class SqlBase : 
    def __init__( self ) :
        self.Clear()
        self.indent = 0
        
    def Add( self, string ) :
        self.sql = self.sql + string
        
    def Clear( self ) :
        self.sql = ""
        
    def AddNewlines( self, n = 2 ) :
        self.sql = self.sql +'\n' * n

#########################################
# Row format
#########################################

class RowFormat( SqlBase ) :
    def __init__( self ) :
        SqlBase.__init__( self )
        
HiveCharMap = { '\n' : '\\n', '\t' : '\\t', '\r' : '\\r' }

def HiveCharRepr( char ) :
    # Hive doesn't seem to like hexadecimal escaped control codes as Python repr returns
    # So here we will put them in octal
    if char in HiveCharMap : return HiveCharMap[ char ]
    return char if ord( char ) > 31 and ord( char ) < 127 else '\\%03o' % ord( char )

class DelimitedRowFormat( RowFormat ) :
    def __init__( self, *fields ) :
        RowFormat.__init__( self )
        if len( fields ) == 1 : fields = fields[0]
        ( self.fieldDelimiter, self.collectionDelimiter, self.mapKeyDelimiter, self.lineDelimiter ) = fields
        
    def __str__( self ) :
        self.Clear()
        self.Add( "\nROW FORMAT DELIMITED " )
        if self.fieldDelimiter :
            self.Add( "\n\tFIELDS TERMINATED BY '" + HiveCharRepr( self.fieldDelimiter ) + "' " )
        if self.collectionDelimiter :
            self.Add( "\n\tCOLLECTION ITEMS TERMINATED BY '" + HiveCharRepr( self.collectionDelimiter ) + "' " )
        if self.mapKeyDelimiter :
            self.Add( "\n\tMAP KEYS TERMINATED BY '" + HiveCharRepr( self.mapKeyDelimiter ) + "' " )
        if self.lineDelimiter :
            self.Add( "\n\tLINES TERMINATED BY '" + HiveCharRepr( self.lineDelimiter ) + "' " )
            
        return self.sql
        
    def __repr__( self ) :
        return repr( ( self.fieldDelimiter, self.collectionDelimiter, self.mapKeyDelimiter, self.lineDelimiter ) )

DefaultDelimitedRowFormat = DelimitedRowFormat( '\001', '\002', '\003', '\n' )

#########################################
# Tables and Selects
#########################################

class Table(SqlBase) :
    def __init__( self, name = None, schema = None ) :
        SqlBase.__init__( self )
        self.name = name
        self.schema = [ ]
        self.inscope = False
        if schema :
            self.AddSchema( schema )
        
    def AddColumn( self, column, name = None ) :
#       print "Adding column '" + str( name ) + "' to table "+ str( self.name ) + ": name = " + str( column.name ) + ", table = " + str( column.table ) + ", def = " + str( column )
#       if column.table :
        column = As( column, name if name else column.name )
#       else :
#           column = copy( column )
        column.table = self
        if name : column.name = name
        self.schema.append( column )
        setattr( self, column.name, column )
        return column
        
    def AddSchema( self, *schema ) :
        schema = Columns( PopIfList( schema ) )
        for column in schema : self.AddColumn( column )
        
    def ClearSchema( self ) :
        for column in self.schema :
            delattr( self, column.name )
        self.schema = [ ]
        
    def __str__( self ) :
        return self.name
    
    def Declaration( self ) :
        return ""
        
    def Files( self, recurse ) :
        return { }
        
    def HasTransform( self ) :
        return False
        
    def Tables( self ) :
        return  [ ]
        
    def SetNameIfNeeded( self, query ) :
        pass
        
    def SetInFromClause( self ) :
        pass
        
class Join(Table) :
    LEFT = "LEFT "
    LEFT_OUTER = "LEFT OUTER "
    LEFT_SEMI = "LEFT SEMI "
    RIGHT = "RIGHT "
    RIGHT_OUTER = "RIGHT_OUTER "
    FULL = "FULL "
    FULL_OUTER = "FULL OUTER "

    def __init__( self, left, right, jointype = None ) :
        Table.__init__( self, None )
        self.left = left
        self.jointype = jointype
        self.right = right
        self.conditions = [ ]
        self.left.SetInFromClause()
        self.right.SetInFromClause()
        
    def On( self, condition ) :
        self.conditions.append( condition )
        return self
        
    def __str__( self ) :
        self.Clear()
        self.Add( str( self.left ) )
        self.Add( ( self.jointype if self.jointype else "" ) + "\nJOIN " + str( self.right ) + " " )
        if len( self.conditions ) > 0 :
            self.Add( "\nON " )
            self.Add( " AND ".join( [ str( condition ) for condition in self.conditions ] ) )
            self.Add( " " )

        return self.sql
        
    def Files( self, recurse ) :
        if not recurse : return { }
        return dict( self.left.Files( True ).items() + self.right.Files( True ).items() )
        
    def HasTransform( self ) :
        return self.left.HasTransform() or self.right.HasTransform()
        
    def Tables( self ) :
        return self.left.Tables() + self.right.Tables()
        
    def SetNameIfNeeded( self, query ) :
        self.left.SetNameIfNeeded( query )
        self.right.SetNameIfNeeded( query )

class QualifiedColumn :
    def __init__( self, col, qualifier ) :
        self.column = col
        self.qualifier = qualifier
        
    def QualifiedName( self ) :
        return self.column.QualifiedName() + ' ' + self.qualifier
        
class Ascending(QualifiedColumn) :
    def __init__( self, col ) :
        QualifiedColumn.__init__( self, col, 'ASC' )

class Descending(QualifiedColumn) :
    def __init__( self, col ) :
        QualifiedColumn.__init__( self, col, 'DESC' )       

class SelectBase(Table) :
    def __init__( self, *columns ) :
        Table.__init__( self )
        columns = PopIfList( columns )
        self.select = [ self.AddColumn( column ) for column in columns ]
        self.fromClause = None
        self.transform = None
        self.distribute = [ ]
        self.sort = [ ]
        self.group = [ ]
        self.cluster = [ ]
        self.order = [ ]
        self.where = [ ]
        self.rowFormat = DefaultDelimitedRowFormat
        self.modules = [ ]
        self.code = [ ]
        self.files = [ ]
        self.limit = None
        self.distinct = False
        
        tables = frozenset( [ x for x in [ col.Table() for col in columns ] if x != None ] )
        if len( tables ) == 1 :
            [ table ] = tables
            self.From( table )
        
    def From( self, table ) :
        self.fromClause = table
        table.SetInFromClause()
        return self
        
    def Transform( self, transform, userargs = None, numkeys = 0 ) :
        self.transform = {  'transform' : transform,
                            'userargs'  : userargs,
                            'numkeys'   : numkeys,
                            'input'     : self.schema,
                            'informat'  : DefaultDelimitedRowFormat,
                            'outformat' : DefaultDelimitedRowFormat }
        self.ClearSchema()
        self.AddSchema( transform.schema )
        return self
        
    def AddModule( self, module, copy = True ) :
        self.modules.append( ( module, copy ) )
        
    def AddCode( self, c ) :
        self.code.append( c )
        
    def AddFile( self, f ) :
        self.files.append( f )
        
    def Where( self, where ) :
        if where : self.where.append( where )
        return self
        
    def DistributeBy( self, *distribute ) :
        self.distribute.extend( PopIfList( distribute ) )
        return self
        
    def SortBy( self, *sort ) :
        self.sort.extend( PopIfList( sort ) )
        return self
        
    def GroupBy( self, *group ) :
        self.group.extend( PopIfList( group ) )
        return self
        
    def ClusterBy( self, *cluster ) :
        self.cluster.extend( PopIfList( cluster ) )
        return self
        
    def OrderBy( self, *order ) :
        self.order.extend( PopIfList( order ) )
        return self
        
    def Limit( self, limit ) :
        self.limit = limit
        return self
    
    def SqlFrom( self ) :
        return ( "\nFROM " + str( self.fromClause ) + " " ) if self.fromClause else ""
        
    def SqlSelect( self ) :

        self.inscope = True
        if self.fromClause : self.fromClause.inscope = True

        self.Clear()
        self.Add( "\nSELECT " )
        if self.distinct :
            self.Add( "DISTINCT " )
        
        if self.transform :
            if not isinstance( self.fromClause, SelectBase ) :
                print "HiPy: Warning: Did you mean to specify a transform on the map side of a map-reduce ?"
            self.Add( "TRANSFORM( " )
            numkeys = self.transform[ 'numkeys' ]
            if len( self.transform['input'] ) > 0 :
                self.Add( ", ".join( [ col.Define( False ) for col in self.transform['input'] ] ) + " ) " )
                input = self.transform[ 'input' ]
            else :
                self.Add( "* ) " )
                input = self.fromClause.schema
            
            self.Add( str( self.transform[ 'informat' ] ) )
            
            transform = self.transform['transform']
            if inspect.isfunction( transform ) or inspect.isclass( transform ) :
                ( self.source, self.script ) = self.CreateTransformDriverScript( transform, self.modules, self.code,
                                                                self.transform['userargs'],
                                                                ( SchemaToPython( input[0:numkeys] ),
                                                                SchemaToPython( input[numkeys:] ),
                                                                SchemaToPython( self.schema ),
                                                                self.transform[ 'informat' ],
                                                                self.transform[ 'outformat' ] ) )

                self.Add( "\nUSING 'python " + self.source + "' " )
            else :
                self.Add( "\nUSING '" + function + "' " )
                
            self.Add( "AS " + SchemaToSql( self.schema ) )
            self.Add( str( self.transform[ 'outformat' ] ) )
            
        elif self.select :
            self.Add( ", ".join( [ col.Define() for col in self.select ] ) + " " )
            
        else :
            self.Add( "* " )
            self.AddSchema( self.fromClause.schema )
            
        if len( self.where ) > 0 :
            self.Add( "\nWHERE " )
            self.Add( " AND ".join( [ Bracket( str( where ) ) for where in self.where ] ) )
        
        if self.distribute  : self.Add( "\nDISTRIBUTE BY " + ", ".join( [ col.QualifiedName() for col in self.distribute ] ) + " " )
        if self.group       : self.Add( "\nGROUP BY " + ", ".join( [ str( col ) for col in self.group ] ) + " " )
        if self.cluster     : self.Add( "\nCLUSTER BY " + ", ".join( [ col.QualifiedName()for col in self.cluster ] ) + " " )
        if self.order       : self.Add( "\nORDER BY " + ", ".join( [ col.QualifiedName()for col in self.order ] ) + " " )
        if self.sort        : self.Add( "\nSORT BY " + ", ".join( [ col.QualifiedName() for col in self.sort ] ) + " " )
        
        if self.limit :
            self.Add( "\nLIMIT " + str( self.limit ) + " " )
        
        self.inscope = False
        if self.fromClause : self.fromClause.inscope = False

        return self.sql
    
    def __str__( self ) :
        
        sql = self.SqlFrom() + self.SqlSelect()

        return ( "( " + sql + ") " + self.name + " " ) if self.name else sql
        
    def CreateTransformDriverScript( self, transform, modules, code, userargs, argtuple ) :
        transformname = transform.__name__
        source = transformname + ".drvr.py"

        sourcecode = ""

        code = [ transform ] + code
        for object in code :
            if inspect.isclass( object ) :
                for cls in reversed( inspect.getmro( object ) ) :
                    sourcecode += inspect.getsource( cls )
                    modules.extend( cls.modules if hasattr( cls, 'modules' ) else [ ] )
            else :
                sourcecode += inspect.getsource( object )
                modules.extend( object.modules if hasattr( object, 'modules' ) else [ ] )

        script =    "#!/usr/bin/python\n"               \
                    "import sys, os\n"                  \
                    "sys.path.append(os.getcwd())\n"    \
                    "import HiPy\n"
                    
        if len( modules ) > 0 :
            script += "import " + ', '.join( set( [ module.__name__ for ( module, copy ) in modules ] ) ) +'\n'
        
        script += sourcecode
        
        script += "if __name__ == '__main__' :\n"
                    
        if inspect.isfunction( transform ) :
            script += "    HiPy.Transform( HiPy.TransformWrapper, " \
                            + "( " + transformname + ", " + repr( userargs ) + " )" \
                            + ", " + repr( argtuple ) + " )\n"
        else :
            script += "    HiPy.Transform( " + transformname + ", " + repr( userargs ) + ", " + repr( argtuple ) + " )\n"
            
        return ( source, script )
        
    def Files( self, recurse ) :
        files = dict( ( file, None ) for file in self.files )
        if self.transform :
            files[ self.source ] = self.script
            if hasattr( self.transform['transform'], 'files' ) :
                for file in self.transform['transform'].files :
                    files[ os.path.abspath( os.path.join( os.path.dirname( inspect.getsourcefile( self.transform['transform'] ) ), file ) ) ] = None
        for ( module, copy ) in self.modules :
            if copy and hasattr( module, '__file__' ) :
                files[ module.__file__ ] = None
        if recurse and self.fromClause :
            files = dict( files.items() + self.fromClause.Files( True ).items() )
        return files
        
    def Tables( self ) :
        return self.fromClause.Tables() if self.fromClause else [ ]
        
    def HasTransform( self ) :
        return self.transform or ( self.fromClause and self.fromClause.HasTransform() )

class Select(SelectBase) :
    def __init__( self, *columns ) :
        SelectBase.__init__( self, *columns )
        self.AddToDefaultQuery()
        self.iterator = None
        self.dir = None
        self.table = None
        
    def Execute( self ) :
        if not self.query : self.AddToDefaultQuery()
        self.query.Execute()
        
    def __iter__( self ) :      
        self.Execute()
        
        for x in os.listdir( self.dir ) :
            f = self.dir + "/" + x
            if os.stat( f ).st_size == 0 or fnmatch.fnmatch( x, "*.crc" ) :
                os.remove( f )
        
        outputfiles = glob.glob( self.dir + "/*" )
        if len( outputfiles ) > 0 :
            #rows = subprocess.Popen( [ "cat" ] + outputfiles, stdout=subprocess.PIPE )
            #self.iterator = InputIterator( rows.stdout, self.rowFormat, self.schema )
            
            self.iterator = InputIterator( fileinput.input( outputfiles ), self.rowFormat, self.schema )
            
        else :
            self.iterator = [ ].__iter__()
        
        return self.iterator
        
    def GetResults( self, targetdir, copy = False ) :
        self.Execute()
        
        if not copy :
            try:
                os.symlink( self.dir, targetdir )
            except:
                copy = True
        
        if copy :
            shutil.copytree( self.dir, targetdir )
            
        
    def WriteToTable( self, table, partition = None ) :
        self.table = table
        self.partition = partition
        if not self.query : self.AddToDefaultQuery()
        
    def Tables( self ) :
        return SelectBase.Tables( self ) + ( [ self.table ] if self.table else [ ] )
        
    def SetNameIfNeeded( self, query ) :
        if not self.name :
            self.name = query.NextSelectName()
        if self.fromClause :
            self.fromClause.SetNameIfNeeded( query )
        
    def AddToDefaultQuery( self ) :
        self.query = DefaultQuery
        self.query.AddSelect( self )
        
    def SetInFromClause( self ) :
        if self.query :
            self.query.RemoveSelect( self )
            self.query = None

class NewTable( Table ) :
    def __init__( self, name, schema,               \
                            ifNotExists = False,    \
                            partitions = None,      \
                            location = None,        \
                            rowFormat = None,       \
                            storedAs = None ) :
        Table.__init__( self, name, schema )
        self.ifNotExists = ifNotExists
        self.partitions = Columns( partitions )
        self.location = location
        self.rowFormat = rowFormat
        self.storedAs = storedAs
        
    def Declaration( self ) :
        self.Clear()
        self.Add( "\nCREATE " + ( "EXTERNAL " if self.location else "" ) + "TABLE " )
        self.Add( ( "IF NOT EXISTS " if self.ifNotExists else "" ) + self.name + "\n" )
        self.Add( SchemaToSql( self.schema ) )
        if self.partitions :
            self.Add( "\nPARTITIONED BY " + SchemaToSql( self.partitions ) )
        if self.rowFormat :
            self.Add( str( self.rowFormat ) )
        if self.storedAs :
            self.Add( "\nSTORED AS " + self.storedAs + " " )
        if self.location :
            self.Add( "\nLOCATION '" + self.location + "'" )
        self.Add( ";" )
        
        return self.sql
        
    def Tables( self ) :
        return [ self ]

#########################################
# Query management
#########################################

class QueryBase( SqlBase ) :
    def __init__( self ) :
        SqlBase.__init__( self )
        self.options = [ ]
        self.files = { }
        self.jars = [ ]
        self.archives = [ ]
        self.selects = set()
        self.dirvar = '${querydir}'
        
        self.nextSelectName = "a"
        
    def AddFile( self, file ) :
        ( name, source ) = file if isinstance( file, tuple ) else ( file, None )
        self.files[ name ] = source
        
    def AddFiles( self, files ) :
        for name in files : self.files[name] = files[name]

    def AddJar( self, file ) :
        self.jars.append( file )
    
    def AddArchive( self, file ) :
        self.archives.append( file )
        
    def AddSelect( self, select ) :
        self.selects.add( select )
        select.query = self
        
    def RemoveSelect( self, select ) :
        self.selects.remove( select )
        select.query = None
        
    def Select( self, *columns ) :
        select = Select( *columns )
        self.AddSelect( select )
        return select
    
    def NextSelectName( self ) :
        result = self.nextSelectName
        self.nextSelectName = chr( ord( self.nextSelectName ) + 1 )
        return result
        
    def SetOption( self, name, value ) :
        self.options.append( ( name, value ) )
    
    def SqlTables( self ) :
        sql = ""
    
        for select in self.selects :
            for table in select.Tables() :
                sql = sql + table.Declaration()
        
        return sql
    
    def SqlQueries( self ) :
        
        # Group by common FROM tables
        queries = { }
        for select in self.selects :
            frm = select.fromClause
            if frm in queries :
                queries[ frm ].append( select )
            else :
                queries[ frm ] = [ select ]
                
        sqls = [ ]
        sql = ""
                                
        for frm in queries :

            sql = queries[ frm ][ 0 ].SqlFrom()
            
            for select in queries[ frm ] :
        
                if select.dir :
                    sql = sql + "\nINSERT OVERWRITE LOCAL DIRECTORY '" + select.dir + "'"
                else :
                    sql = sql + "\nINSERT OVERWRITE TABLE " + str( select.table )
                    if select.partition :
                        sql = sql + " PARTITION( " + select.partition + ")"
            
                sql = sql + select.SqlSelect()
                
                self.AddFiles( select.Files( False ) )
            
            sqls.append( sql )
            
            self.AddFiles( frm.Files( True ) )
            
        return ";\n".join( sqls )
                    
    def __str__( self ) :
    
        self.Clear()
        
        self.Add( ''.join( [ "SET " + name + "=" + str( value ) + ";\n" for ( name, value ) in self.options ] ) )
        
        queries = self.SqlQueries()
        
        self.Add( self.SqlTables() )
        
        if len( self.files ) > 0 : self.Add( "\nADD FILES " + " ".join( [ self.MakePath( filename ) for filename in self.files ] ) + ";\n" )
        if len( self.jars ) > 0 : self.Add( "\nADD JARS " + " ".join( [ self.MakePath( jar ) for jar in self.jars ] ) + ";\n" )
        if len( self.archives ) > 0 : self.Add( "\nADD ARCHIVES " + " ".join( [ self.MakePath( archive ) for archive in self.archives ] ) + ";\n" )
    
        self.Add( queries )
        
        return self.sql
        
    def MakePath( self, filename ) :
        return os.path.join( self.dirvar, os.path.expanduser( filename ) )
                
class Query(QueryBase) :
    # The directory is used as a workspace for execution of the query 
    # hive is a function which executes a hive query, given the filename 
    def __init__( self, dir, hive = None ) :
        QueryBase.__init__( self )
        self.hive = hive
        self.didExecute = False
        self.cache = True
        self.cwd = os.getcwd()
        self.SetCacheDirectory( dir )
        self.querydir = None
        self.resultdir = None
                    
    def SetCacheDirectory( self, dir ) :
        if dir[0] == '/' :
            self.dir = dir
        elif dir[0] == '~' :
            self.dir = os.path.expanduser( dir )
        else :
            self.dir = self.cwd + '/' + dir
            
        self.tmpdir = self.dir + '/tmp'
        if not os.path.exists( self.tmpdir ) :
            os.makedirs( self.tmpdir )

    def CreateQueryDirectories( self ) :
        self.querydir = tempfile.mkdtemp( prefix = self.hashstr, dir = self.tmpdir )
        for select in self.selects :
            if select.dir :
                dir = select.dir.replace( self.dirvar, self.querydir )
                if not os.path.exists( dir ) :
                    os.mkdir( dir )
    
    def Hash( self ) :
        queryscripts = "\n".join( [ self.query.replace( ".pyc", ".py" ) ] + [ script for script in self.files.itervalues() if script ] )
        return struct.pack( "!I", zlib.crc32( queryscripts ) & 0xffffffff ).encode( 'base64' )[0:6].replace( '/', '=' )
        
    def IsCached( self ) :
        if not self.cache :
            return False
        
        oldruns = [ name for name in os.listdir( self.dir ) if name.startswith( self.hashstr ) ]
        if len( oldruns ) > 0 :
            self.resultdir = self.dir + '/' + oldruns[ len( oldruns ) - 1 ]
            return True
            
    def CreateQueryFiles( self ) :
        self.queryfile = self.querydir + "/query.q"
        with open( self.queryfile, "w" ) as f :
            f.write( self.query )
        
        # Create the script files
        for ( filename, script ) in self.files.iteritems() :
            filepath = self.querydir + '/' + filename
            if script :
                with open( filepath, "w" ) as f :
                    f.write( script )
                    
    def RenameQueryDir( self ) :
        self.resultdir = self.dir + '/' + self.hashstr + "-" + datetime.datetime.today().strftime( "%Y%m%d-%H%M" )
        os.rename( self.querydir, self.resultdir )
        self.querydir = None
        
    def Execute( self ) :
        if not self.didExecute :
            cwd = os.getcwd()
        
            # Decide sub-directory names (if necessary) for the selects
            for select in self.selects :
                select.SetNameIfNeeded( self )
                if not select.dir and not select.table :
                    select.dir = self.dirvar + '/' + select.name
                
            # Add a file for HiPy.py if necessary
            if not all( [ not select.HasTransform() for select in self.selects ] ) :
                self.AddFile( HiPyPath )
            
            # Generate the SQL and driver script source
            self.query = str( self )
            self.hashstr = self.Hash()
            
            print self.query
            print "Script hash is: " + self.hashstr
            
            # If not cached, run the query and move the results to a subdir with the hash-based name
            if not self.IsCached() :
                # Create a tmp directory for this run
                self.CreateQueryDirectories()
            
                # Create the query file
                self.CreateQueryFiles()
                        
                # Run the query
                returncode = self.hive( self.queryfile, params = [ '-d', self.dirvar[2:len(self.dirvar)-1]+ '=' + self.querydir ] )
                if returncode != 0 :
                    raise HiPyException( "Hive error" )
                
                # Rename the directory now it's no longer an attempt
                self.RenameQueryDir()
            
            for select in self.selects :
                if select.dir :
                    select.dir = select.dir.replace( self.dirvar, self.resultdir )
                
            # Done!
            self.didExecute = True
            
            os.chdir( cwd )
            
    def Clean( self, all ) :
        if self.didExecute :
            if self.querydir :
                shutil.rmtree( self.querydir, ignore_errors = True )
            if self.resultdir :
                shutil.rmtree( self.resultdir, ignore_errors = True )
        if all :
            shutil.rmtree( self.tmpdir, ignore_errors = True )
            
    def Cache( self, cache ) :
        self.cache = cache

#########################################
# Default Query
#########################################

DefaultQuery = Query( '~/.hipy' )

def SetHive( hive ) :
    DefaultQuery.hive = hive
    
def Reset() :
    global DefaultQuery
    DefaultQuery = Query( '~/.hipy', DefaultQuery.hive )
    
def Execute() :
    DefaultQuery.Execute()
    
def Cache( cache ) :
    DefaultQuery.Cache( cache )
    
def SetOption( name, value ) :
    DefaultQuery.SetOption( name, value )
    
def Clean( all = False ) :
    DefaultQuery.Clean( all )
    

#########################################
# Describe tables
#########################################   

class DescribeHandler :
    def __init__( self ) :
        pass
        
    def __call__( self, output, error ) :
        self.schema = SchemaFromDescribe( output )

def Describe( tablename, hive = None ) :
    if hive == None :
        hive = DefaultQuery.hive
        
    handler = DescribeHandler()
    returncode = hive( script = "describe "+tablename, handler = handler )
    if returncode != None :
        return None
        
    return Table( tablename, handler.schema )

#########################################
# Data row object
#########################################
class DataRow :
    def __init__( self, data, schema ) :
        self.value = data
        self.schema = schema
    
    def __len__( self ) :
        return len( self.value )
        
    def __getitem__( self, key ) :
        if isinstance( key, str ) :
            for idx in range(0, len( self.schema ) ):
                if self.schema[idx].name == key :
                    return self.value[idx]
            error = " ".join( [ key, "not found in (", ", ".join( x.name for x in self.schema ), ")" ] )
            raise KeyError( error )
            
        if isinstance( key, slice ) :
            return DataRow( self.value[ key ], self.schema[ key ] )
        
        return self.value[ key ]
            
    def __setitem__( self, key, value ) :
        if isinstance( key, str ) :
            for idx in range(0, len( self.schema ) ):
                if self.schema[idx].name == key :
                    self.value[idx] = value
                    return
            raise KeyError

        self.value[ key ] = value
    
    def __iter__( self ) :
        return self.value.__iter__()
        
    def __reversed__( self ) :
        return self.value.__reversed__()
        
    def __eq__( self, other ) :
        if isinstance( other, DataRow ) :
            return self.value == other.value
        return self.value == other
        
    def __ne__( self, other ) :
        return not self.__eq__( other )
        
    def dict( self ) :
        return dict( ( col.name, value ) for ( col, value ) in zip( self.schema, self.value ) )
        
    def __str__( self ) :
        return str( self.value )
        
#########################################
# Input/output processing
#########################################

def ReadHiveType( value, type, rowFormat ) :

    if value == '\N' : return None

    if isinstance( type, HiveArray ) :
        out = [ ReadHiveType( element, type.datatype, rowFormat ) for element in value.split( rowFormat.collectionDelimiter ) ]
    elif isinstance( type, HiveMap ) :
        out = { }
        elements = value.split( rowFormat.collectionDelimiter )
        for element in elements :
            keyvalue = element.split( rowFormat.mapKeyDelimiter )
            key = ReadHiveType( keyvalue[0], type.keytype, rowFormat )
            val = ReadHiveType( keyvalue[1], type.datatype, rowFormat )
            out[key] = val
    elif isinstance( type, HiveStruct ) :
        out = [ ReadHiveType( element, elementtype, rowFormat ) \
                    for ( element, ( key, elementtype ) )           \
                    in zip( value.split( rowFormat.collectionDelimiter ), type.schema ) ]
    elif type == HiveTinyInt or type == HiveSmallInt or type == HiveInt or type == HiveBigInt :
        out = int( float( value ) )
    elif type == HiveBigInt :
        out = long( float( value ) )
    elif type == HiveBoolean :
        out = bool( value )
    elif type == HiveFloat or type == HiveDouble :
        out = float( value )
    elif type == HiveString :
        out = str( value )
    elif type == HiveJson :
        try :
            out = JsonLoads( value )
        except ValueError :
            out = None
    else :
        raise TypeError
        
    return out
    
def WriteHiveType( value, type, rowFormat ) :
    if isinstance( type, HiveArray ) :
        out = rowFormat.collectionDelimiter.join( [ WriteHiveType( element, type.datatype, rowFormat ) for element in value ] )
    elif isinstance( type, HiveMap ) :
        out = rowFormat.collectionDelimiter.join( [ ( WriteHiveType( key, type.keytype, rowFormat ) + \
                                                      rowFormat.mapKeyDelimiter + \
                                                      WriteHiveType( val, type.datatype, rowFormat ) )  \
                                                    for ( key, val ) in value.items() ] )
    elif isinstance( type, HiveStruct ) :
        out = rowFormat.collectionDelimiter.join( [ WriteHiveType( element, elementtype )   \
                                                    for ( element, ( name, elementtype ) )  \
                                                    in zip( value, type.schema ) ] )
    elif type == HiveJson :
        out = JsonDumps( value )
    elif value == None :
        out = '\\N'
    else :
        out = str( value )
    
    return out
            
def Deserialize( hiverow, rowFormat, schema ) : 
    value =  [ ReadHiveType( field.rstrip( rowFormat.lineDelimiter ), col.type, rowFormat ) \
        for ( field, col )                  \
        in zip( hiverow.split( rowFormat.fieldDelimiter ), schema ) ]
    return DataRow( value, schema )
        
def Serialize( datarow, rowFormat, schema ) :
    return rowFormat.fieldDelimiter.join( [ WriteHiveType( value, col.type, rowFormat )         \
                                                    for ( value, col ) in zip( datarow, schema ) ] )    \
                                + rowFormat.lineDelimiter

class InputIterator :
    def __init__( self, iterator, rowFormat, schema ) :
        self.iterator = iterator
        self.rowFormat = rowFormat
        self.schema = schema
        self.nextrow = None
        
    def __iter__( self ) :
        return self
        
    def next( self ) :
        return Deserialize( self.iterator.next(), self.rowFormat, self.schema )
        
class Output :
    def __init__( self, rowFormat, schema ) :
        self.rowFormat = rowFormat
        self.schema = schema
        
    def __call__( self, row ) :
        return Serialize( row, self.rowFormat, self.schema )
        

#########################################
# Transform driver
#########################################

class PeekableIterator :
    def __init__( self, iterator ) :
        self.iterator = iterator
        self.nextitem = None
        
    def __iter__( self ) :
        return self
        
    def next( self ) :
        if not self.nextitem : return self.iterator.next()

        nextitem = self.nextitem
        self.nextitem = None
        return nextitem
        
    def peek( self ) :
        if not self.nextitem : self.nextitem = self.iterator.next()
        return self.nextitem
                
    def put_back( self, item ) :
        if self.nextitem : raise "Can't put_back more than once!"
        self.nextitem = item

class GroupIterator :
    def __init__( self, key, input ) :
        self.key = key
        self.input = input
        
    def __iter__( self ) :
        return self
        
    def next( self ) :
        nextrow = self.input.next()
        if nextrow[0:len(self.key)] == self.key :
            return nextrow[len(self.key):]
            
        self.input.put_back( nextrow )
        
        raise StopIteration
        
class GroupByKey :
    def __init__( self, numkeys, input ) :
        self.numkeys = numkeys
        self.input = PeekableIterator( input.__iter__() )
        
    def __iter__( self ) :
        return self
        
    def next( self ) :
        key = self.input.peek()[0:self.numkeys]
        return ( key, GroupIterator( key, self.input ) )

class TransformWrapper :
    def __init__( self, fnargs ) :
        ( self.function, self.userargs ) = fnargs
        
    def __call__( self, keys, input ) :
        return self.function( self.userargs, keys, input )

def Transform( transform, userargs, argtuple ) :

    infile = sys.stdin
    outfile = sys.stdout

    ( inkeyschema, inschema, outschema, informat, outformat ) = argtuple
    
    input = InputIterator( infile, DelimitedRowFormat( informat ), Columns( inkeyschema + inschema ) )
    output = Output( DelimitedRowFormat( outformat ), Columns( outschema ) )
    
    transformobj = transform( userargs )
    
    if len( inkeyschema ) != 0 :
        for ( key, values ) in GroupByKey( len(inkeyschema), input ) :
            if len( key ) != len( inkeyschema ) :
                sys.stderr.write( "Key error: key=" + repr( key ) + ", schema=" + repr( inkeyschema ) + "\n" )
            else :
                for out in transformobj( key, values ) :
                    outfile.write( output( out ) ) 
                
    else :
        for out in transformobj( None, input ) :
            outfile.write( output( out ) ) 

###################################
# Configure from file
###################################

# Configuration object contains
#   'tables' : Dictionary of the table objects

class Configuration :
    def __init__( self, dir, name, module ) :
        self.dir = os.path.expanduser( dir )
        self.filename = name
        self.path = self.dir + '/' + self.filename
        self.module = module
        self.config = None
        self.loading = False

    def Initialize( self ) :
        self.config = { 'tables': { } }

    def Load( self ) :
        if self.config :
            return self.config
            
        if self.loading :
            return None

        try :
            configfile = open( self.path, 'r' )
        except IOError :
            print "HiPy: No configuration found ..."
            return None
        
        try :
            self.loading = True
            self.config = pickle.load( configfile )
            self.loading = False
        except :
            print "HiPy: Configuration load error ..."
            return None
        
        configfile.close()
        print "HiPy: Configuration loaded."
        
        return self.config
        
    def Save( self ) :
        if not self.config :
            return
        
        if not os.path.exists( self.dir ) :
            os.makedirs( self.dir )
        
        try :
            configfile = open( self.path, 'w' )
        except IOError :
            print "HiPy: Configuration save error (1) ..."
            return
            
        try :
            pickle.dump( self.config, configfile )
        except :
            print "HiPy: Configuration save error (2) ..."
            return
            
        configfile.close()
        
        print "HiPy: Configuration saved."

    def Configure( self ) :
        if not self.Load() :
            self.Initialize()
            self.Save()
        
    def Update( self ) :
        if not self.Load() :
            self.Initialize()
            self.ReadTablesFromHive( default_hive_tables )
        else :
            self.ReadTablesFromHive( self.config[ 'tables' ].keys() )
            
        self.ReadDevicesFromDataoven()
        self.Save()
        self.AddTablesToModule()
        
    def ReadTablesFromHive( self, tablelist ) :
        tables = self.config[ 'tables' ]
        for tablename in tablelist :
            print "HiPy: Reading schema for " + tablename + " from Hive"
            tables[ tablename ] = HiPy.Describe( tablename )
    
    def GetTables( self ) :
        return self.config[ 'tables' ]
        
    def GetTable( self, tablename ) :
        tables = self.config[ 'tables' ]
        if tablename in tables :
            return tables[ tablename ]
            
        table = Describe( tablename )
        if table is not None :
            tables[ tablename ] = table
            self.Save()

        return table

########################################
# Configuration commands
########################################

def Update() :
    global config
    return config.Update()
        
def ShowTables() :
    for table in GetTables().iterkeys() :
        print table
        
def ShowTable( arg ) :
    table = GetTable( arg )
    if table is not None :
        for col in table.schema :
            print col.name, str( col.type )
    else :
        print "Unknown table: ", arg

def GetTable( tablename ) :
    global config
    return config.GetTable( tablename )

def GetTables() :
    global config
    return config.GetTables()
    
########################################
# Json parsing
######################################## 

JsonLoads = json.loads
JsonDumps = json.dumps

def SetJsonParser( loads, dumps ) :
    global JsonLoads
    global JsonDumps
    
    JsonLoads = loads
    JsonDumps = dumps

########################################
# main() function for command line usage
########################################        

def main() :
    import HiPy
    
    usage = "usage: %prog [options] command\n\n" \
            "Commands:\n" \
            "   showtables - show configured tables\n" \
            "   showtable <table name> - show the schema of a table"
            
    options = OptionParser( usage )
    
    ( options, args ) = options.parse_args()
    
    if len( args ) < 1 :
        sys.stderr.write( "Must provide a command\n" )
        sys.exit( 1 )
        
    zeroargs = { 'showtables' : HiPy.ShowTables }
    oneargs = { 'showtable' : HiPy.ShowTable }
                
    command = args[0].lower()
    
    if command in zeroargs :
        zeroargs[ command ]()
    elif command in oneargs :
        oneargs[ command ]( args[1] )
    else :
        print "Unrecognized command: ", args[0]
                        
if __name__ == "__main__" :
    main()
else :
    config = Configuration( "~/.hipy", "config", sys.modules[__name__] )
    config.Configure()

