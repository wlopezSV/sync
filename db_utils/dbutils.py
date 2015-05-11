#-----------------------------------------------------------------------------------------------
# Name:         dbutils.py
# Purpose:      Realizar operaciones sobre Bases de Datos
#
# Author:      William Lopez
#
# Created:     08/05/2015
#-----------------------------------------------------------------------------------------------
''' Módulo dbutils para realizar operaciones basicas sobre sqllite3

    Clases:
        clsSQLite -> Manejo de sqlite3
'''
__author__      = "William Lopez"
__copyright__ = ""
__credits__ = ["", "", "",""]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "William Lopez"
__email__ = "wlopez.a@gmail.com"
__status__ = "Development"


#-------------------------------------------------------------------------------
#Importacion de Modulos
#-------------------------------------------------------------------------------
import sqlite3

#-------------------------------------------------------------------------------
#Clase: clsSQLite
#-------------------------------------------------------------------------------
class clsSQLite(object):
    ''' Clase para realizar operaciones basicas sobre bases de datos sqlite

    Propiedades:
        database <string>   : Ruta y nombre de la base de datos

    Metodos:
        execsql(sql,args()) : Ejecuta una sentencia sql que no retorna resultados
        getresults(sql,args=(),one=False) : Ejecuta una sentencia sql que retorna uno o varios resultados
    '''

    def __init__(self,database=""):
        self.database = database

    def execsql(self,sql,args=()):
        ''' Ejecutar una sentencia SQL que no retorna ningun resultado

        Argumentos:
            sql <string> -> Sentencia sql, para pasar partes variables usar ?
            args <lista> -> Argumentos como Lista, opcional
        '''
        try:
            db = sqlite3.connect(self.database)
            #cur = db.cursor()
            db.text_factory = str
            db.execute(sql,args)
            db.commit()
            db.close()
        except sqlite3.Error, e:
            print "Error %s:" % e.args[0]
            db.rollback()
        db.close()

    def getresults(self,sql,args=(),one=False):
        ''' Ejecuta una sentencia SQL que retorna uno o más resultados

        Argumentos:
            sql <string>  -> Sentencia sql, para pasar partes variables usar ?
            args <lista>  -> Argumentos como Lista, opcional
            one <boolean> -> True: Devuelve un dato como resultado
                             False: Retorna filas

        Retorna:
            resultados
        '''
        try:
            db = sqlite3.connect(self.database)
            db.text_factory = str
            db.row_factory = sqlite3.Row
            cur = db.cursor()
            cur.execute(sql,args)
            if one:
                resultados = cur.fetchone()[0]
            else:
                resultados = cur.fetchall()
            db.close()
        except sqlite3.Error, e:
            print "Error %s:" % e.args[0]
            db.rollback()
        db.close()

        return resultados