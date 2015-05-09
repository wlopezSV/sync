#-----------------------------------------------------------------------------------------------
# Name:         dbutils.py
# Purpose:      Realizar operaciones sobre Bases de Datos
#
# Author:      William Lopez
#
# Created:     08/05/2015
#-----------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------
#Importacion de Modulos
#-------------------------------------------------------------------------------
import sqlite3

#-------------------------------------------------------------------------------
#Clase: clsSQLite
#-------------------------------------------------------------------------------
class clsSQLite(Object):
    ''' Clase para realizar operaciones basicas sobre bases de datos sqlite
    '''

    def __init__(self,database=""):
        self.database = database

    def execsql(sql,args=()):
        ''' Ejecutar una sentencia SQL que no retorna ningun resultado
        Argumentos:
            sql -> Sentencia sql, para pasar partes variables usar ?
            args -> Argumentos como Lista, opcional
        '''
        try:
            db = sqlite3.connect(self.database)
            cur = db.cursor()
            db.text_factory = str
            db.execute(sql,args)
            db.commit()
            db.close()
        except sqlite3.Error, e:
            print "Error %s:" % e.args[0]
            db.rollback()
        finally:
            db.close()


