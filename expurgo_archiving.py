import sys
import datetime
import psycopg2
import time
import decimal

class ExpurgoArchiving:
    def __init__(self, dbname, user, password, host_origem, host_destino, port):
        self.DEBUG = 0
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host_origem = host_origem
        self.host_destino = host_destino
        self.port = port
        self.cursor_origem = None
        self.cursor_destino = None
        self.path_dict = None
        self.principal_table = None
        self.principal_values = None
        self.data_inicio = None
        self.data_final = None
        self.step = 100
        self.step_pedidos = 10 # tabela muito pesada deleta 10 por vez

        self.connection_destino = psycopg2.connect(
            dbname=dbname + '_archiving',
            user=user,
            password=password,
            host=host_destino,
            port=port
        )
        self.cursor_destino = self.connection_destino.cursor()

        self.connection_origem = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host_origem,
            port=self.port
        )
        self.cursor_origem = self.connection_origem.cursor()

    def run(self, table, custom_path = False):
        schema = 'public'
        if '.' in table:
            schema = table.split('.')[0]
            table = table.split('.')[1]

        tablestring = f'{schema}.{table}'

        self.principal_table = table
        self.path_dict = self.gera_dict_path(table, schema)
        self.ajusta_index(table, schema)
        
        if not self.path_dict and custom_path:
            self.path_dict = custom_path
        
        self.cursor_destino.execute(f"select id, datahora_inicio_archiving, datahora_final_archiving, quantidade_inserida from log_archiving where tipo_alteracao = 'INSERT' and tabela = '{tablestring}' and excluido_origem = false order by id limit 1")
        log = self.cursor_destino.fetchone()
        if not log:
            return False
        
        id_archiving = log[0]
        datahora_archiving = log[1]
        datahora_final_archiving = log[2]

        self.data_inicio = datahora_archiving
        self.data_final = datahora_final_archiving
        
        self.cursor_destino.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}'  and table_schema = '{schema}' order by ordinal_position ) x")
        colunas = self.cursor_destino.fetchone()[0]
        colunas = colunas.split(', ')
        colunas_str = ', '.join(colunas)

        datainicio = datetime.datetime.now()
        datainicio = datainicio.strftime("%Y-%m-%d %H:%M:%S")
        self.cursor_destino.execute(f"select {colunas_str} from {tablestring} where datahora_archiving = '{datahora_archiving}'")
        dados = self.cursor_destino.fetchall()


        if len(dados) > 0:
            valores = ', '.join(str(d[colunas.index('id')]) for d in dados)
            self.principal_values = valores

            if self.path_dict:
                self.delete_data_origem_fks(tablestring, dados, self.path_dict, colunas)

            if self.DEBUG == 1:
                print(f"delete from {tablestring} where id in ({valores[0:10]}...)")
            else:
                print(f"tabela: {tablestring} data_inicio: {datainicio} quantidade: {len(dados)}")

                if table == 'pedidos':
                    ids = valores.split(', ')
                    for i in range(0, len(ids), self.step_pedidos):
                        self.cursor_origem.execute(f"delete from {tablestring} where id in ({', '.join(ids[i:i+self.step_pedidos])})")
                        self.connection_origem.commit()
                else:
                    ids = valores.split(', ')
                    for i in range(0, len(ids), self.step):
                        self.cursor_origem.execute(f"delete from {tablestring} where id in ({', '.join(ids[i:i+self.step])})")
                        self.connection_origem.commit()
                        if not 'fidelize' in self.dbname: 
                            time.sleep(1)
                
                self.cursor_destino.execute(f"UPDATE log_archiving SET excluido_origem = true where id = {id_archiving}")
                self.connection_destino.commit()
                self.log(tablestring, datainicio, 'DELETE', 0, len(dados), id_archiving)

        self.cursor_destino.close()
        self.cursor_origem.close()        
        self.connection_destino.close()
        self.connection_origem.close()

        # print(len(dados))
        return len(dados)
        

    def log(self, tabela, datainicio, tipo_operacao, quantidade_inserida, quantidade_excluida, insert_id):
        datafim = datetime.datetime.now()
        datafim = datafim.strftime("%Y-%m-%d %H:%M:%S")
        
        self.cursor_origem.execute(f"SELECT pg_total_relation_size('{tabela}'), pg_indexes_size('{tabela}');")
        dados_origem = self.cursor_origem.fetchone()
        tamanho_tabelaorigem = dados_origem[0]
        tamanho_tabelaindexorigem = dados_origem[1]
        self.cursor_destino.execute(f"SELECT pg_total_relation_size('{tabela}'), pg_indexes_size('{tabela}');")
        dados_destino = self.cursor_destino.fetchone()
        tamanho_tabeladestino = dados_destino[0]
        tamanho_tabelaindexdestino = dados_destino[1]

        if self.DEBUG == 0:
            self.cursor_destino.execute(f"insert into log_archiving (datahora_inicio_archiving, datahora_final_archiving, tabela, tipo_alteracao, quantidade_inserida, quantidade_excluida, tamanho_tabelaorigem, tamanho_tabelaindexorigem, tamanho_tabeladestino, tamanho_tabelaindexdestino, excluido_origem, insert_id) values ('{datainicio}', '{datafim}', '{tabela}', '{tipo_operacao}', {quantidade_inserida}, {quantidade_excluida}, {tamanho_tabelaorigem}, {tamanho_tabelaindexorigem}, {tamanho_tabeladestino}, {tamanho_tabelaindexdestino}, true, {insert_id})")
            self.connection_destino.commit()
        
    def ajusta_index(self, tabela, schema):
        try:
            self.cursor_destino.execute(f"""SELECT count(1) FROM pg_indexes WHERE tablename = 'log_archiving' 
            and indexdef like '%datahora_inicio%';""")
            l1 = self.cursor_destino.fetchone()[0]
            if l1 < 1:
                self.cursor_destino.execute(f"""CREATE INDEX log_archiving_datahora_inicio_archiving_idx ON public.log_archiving (datahora_inicio_archiving);""")
                self.connection_destino.commit()
            
            
            self.cursor_destino.execute(f"""SELECT count(1) FROM pg_indexes WHERE tablename = '{tabela}' and schemaname = '{schema}'
            and indexdef like '%datahora_archiving%';""")
            l2 = self.cursor_destino.fetchone()[0]
            if l2 < 1:
                self.cursor_destino.execute(f"""CREATE INDEX {schema}_{tabela}_datahora_archiving_idx ON {schema}.{tabela} (datahora_archiving);""")
                self.connection_destino.commit()
        except:
            ...
            

    def ajusta_log(self, table):
        schema = 'public'
        if '.' in table:
            schema = table.split('.')[0]
            table = table.split('.')[1]
        
        tablestring = f'{schema}.{table}'

        if self.DEBUG == 0:
            self.ajusta_index(table, schema)

            self.cursor_destino.execute(f"""
            insert into log_archiving (tabela, datahora_inicio_archiving, quantidade_inserida, quantidade_excluida, tipo_alteracao, datahora_final_archiving)
            select '{tablestring}' tabela, datahora_archiving, count(1) as qtd_inserida, 0, 'INSERT', datahora_archiving from {tablestring} tb where not exists 
            (select 1 from log_archiving la where tb.datahora_archiving = la.datahora_inicio_archiving and la.tabela = '{tablestring}' and la.tipo_alteracao = 'INSERT')
            group by datahora_archiving order by datahora_archiving""")
            self.connection_destino.commit()


            self.cursor_destino.execute(f"""delete from log_archiving la where tabela = '{tablestring}' and tipo_alteracao = 'INSERT' 
            and not exists (select 1 from {tablestring} where datahora_archiving = la.datahora_inicio_archiving)
            """)
            self.connection_destino.commit()

            self.cursor_origem.execute(f"ANALYZE {tablestring};")
            self.connection_origem.commit()


    def delete_data_origem_fks(self, table, dados, path_dict, colunas):
        for tab in path_dict[table]['tabelas_ref']:
            index_refcol = colunas.index(tab['refcoluna'])
            self.ajusta_log(tab['tabela'])
            valores = ', '.join(str(d[index_refcol]) for d in dados)
            # print(tab, valores)
            if valores:
                self.fks_recursivo(tab['tabela'], valores, tab['coluna'], tab['refcoluna'])
                    
        return True
    
    def fks_recursivo(self, table, id_ref, col, refcoluna):
        schema = 'public'
        if '.' in table:
            schema = table.split('.')[0]
            table = table.split('.')[1]

        tablestring = f'{schema}.{table}'

        path = self.gera_dict_path(table, schema)
        
        self.cursor_destino.execute(f"select count(1) from {tablestring} where {col} in ({id_ref})")
        qtd_destino = self.cursor_destino.fetchone()[0]
            
        self.cursor_origem.execute(f"select count(1) from {tablestring} where {col} in ({id_ref})")
        qtd_origem = self.cursor_origem.fetchone()[0]

        self.cursor_destino.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position ) x")
        colunas = self.cursor_destino.fetchone()[0]
        colunas = colunas.split(', ')

        print(f"tabela: {tablestring}, qtd_destino: {qtd_destino}, qtd_origem: {qtd_origem}")

        if qtd_origem > 0 and qtd_destino >= qtd_origem and colunas:
            colunas_str = ', '.join(colunas)

            datainicio = datetime.datetime.now()
            datainicio = datainicio.strftime("%Y-%m-%d %H:%M:%S")

            self.cursor_destino.execute(f"select {colunas_str} from {tablestring} where {col} in ({id_ref}) order by id")
            dados = self.cursor_destino.fetchall()
            datahora_archiving = dados[0][0]

            index_refcol = colunas.index(refcoluna)
            valores_destino = ', '.join(str(d[index_refcol]) for d in dados)           

            if len(dados) > 0:
                self.cursor_destino.execute(f"select id from log_archiving where datahora_inicio_archiving = '{datahora_archiving}' and tabela = '{tablestring}' and quantidade_inserida = {qtd_destino} and excluido_origem = false and tipo_alteracao = 'INSERT'")
                insert_id = self.cursor_destino.fetchone()
                
                if insert_id:
                    insert_id = insert_id[0]
                else:
                    insert_id = None

                if path:
                    self.delete_data_origem_fks(tablestring, dados, path, colunas)
                
                if self.DEBUG == 1:
                    print('insert_id : ', insert_id)
                    print(f"delete from {tablestring} where {refcoluna} in ({valores_destino[0:10]}...)")
                else:
                    
                    if table == 'pedidos':
                        ids = valores_destino.split(', ')
                        for i in range(0, len(ids), self.step_pedidos):
                            self.cursor_origem.execute(f"delete from {tablestring} where {refcoluna} in ({', '.join(ids[i:i+self.step_pedidos])})")
                            self.connection_origem.commit()
                    else:
                        ids = valores_destino.split(', ')
                        for i in range(0, len(ids), self.step):
                            self.cursor_origem.execute(f"delete from {tablestring} where {refcoluna} in ({', '.join(ids[i:i+self.step])})")
                            self.connection_origem.commit()
                            if not 'fidelize' in self.dbname: 
                                time.sleep(5)

                    if insert_id is None:
                        insert_id = 'NULL'
                    else: 
                        self.cursor_destino.execute(f"UPDATE log_archiving SET excluido_origem = true where id = {insert_id}")
                        self.connection_destino.commit()

                    self.log(tablestring, datainicio, 'DELETE', 0, len(dados), insert_id)

        elif qtd_origem > qtd_destino:
            # print('inserindo')
            colunas_origem = ', '.join(colunas[1:])
            
            self.cursor_origem.execute(f"select '{self.data_inicio}' datahora_archiving, {colunas_origem} from {tablestring} where {col} in ({id_ref})")
            dados_origem = self.cursor_origem.fetchall()
            
            self.cursor_destino.execute(f"select '{self.data_inicio}' datahora_archiving, {colunas_origem} from {tablestring} where {col} in ({id_ref})")
            dados_destino = self.cursor_destino.fetchall()

            idx = colunas.index('id')
            
            conjunto1 = set(dados_origem)
            conjunto2 = set(dados_destino)
            
            del dados_origem
            del dados_destino

            itens_diferentes = list(conjunto1 - conjunto2)
            
            del conjunto1
            del conjunto2

            itens_diferentes = [str(x[idx]) for x in itens_diferentes]

            values = ', '.join(itens_diferentes)

            self.cursor_origem.execute(f"select '{self.data_inicio}' datahora_archiving, {colunas_origem} from {tablestring} where id in ({values})")
            dados = self.cursor_origem.fetchall()
            
            self.insert_many(self.connection_destino, self.cursor_destino, dados, tablestring, 'datahora_archiving, '+colunas_origem, refcoluna)
            self.log(tablestring, self.data_inicio, 'INSERT', len(dados), 0, 'NULL')
            
            self.fks_recursivo(tablestring, id_ref, col, refcoluna)
            

    def gera_dict_path(self, table, schema):
        path_dict = {}
        self.cursor_origem.execute(f"""
                SELECT
                    conname AS foreign_key_name,
                    ns1.nspname || '.' || c1.relname AS table_with_foreign_key,
                    a.attname AS column_with_foreign_key,
                    ns2.nspname || '.' || c2.relname AS referenced_table,
                    b.attname AS referenced_column
                FROM
                    pg_constraint
                JOIN
                    pg_attribute AS a ON a.attnum = ANY(pg_constraint.conkey) AND a.attrelid = conrelid
                JOIN
                    pg_attribute AS b ON b.attnum = ANY(pg_constraint.confkey) AND b.attrelid = confrelid
                JOIN
                    pg_class AS c1 ON c1.oid = conrelid
                JOIN
                    pg_namespace AS ns1 ON ns1.oid = c1.relnamespace
                JOIN
                    pg_class AS c2 ON c2.oid = confrelid
                JOIN
                    pg_namespace AS ns2 ON ns2.oid = c2.relnamespace
                WHERE
                    ns2.nspname = '{schema}' AND c2.relname = '{table}';
            """)

        referencing_tables = self.cursor_origem.fetchall()
        
        for referencing_table in referencing_tables:
            referencing_table_name = referencing_table[3]
            if referencing_table_name not in path_dict:
                path_dict[referencing_table_name] = {
                    'tabelas_ref': [{
                        'tabela': referencing_table[1],
                        'coluna': referencing_table[2],
                        'refcoluna': referencing_table[4]
                    }]
                }
            else:
                path_dict[referencing_table_name]['tabelas_ref'].append({
                    'tabela': referencing_table[1],
                    'coluna': referencing_table[2],
                    'refcoluna': referencing_table[4]
                })
        # print(path_dict)

        return path_dict

    
    def limpar_dados(self, n):
        if isinstance(n, list):
            lista = '['
            for m in n:
                dado = str(m)
                if not (type(m) == int or type(m) == float or type(m) == decimal.Decimal):
                    dado = self.limpar_dados(m)
                lista += dado + ','
            lista = lista[:-1] + ']'
            return "ARRAY%s" % str(lista)
        elif(type(n) == datetime):
            return f"'{str(n)[:23]}'"
        else:
            return "'%s'" % str(n).replace("'", "''") if str(n) != 'None' else 'NULL'

        
    def insert_many(self, conn, cursor, dados, tabela, colunas, refcoluna, step = 1000):
        sql = " insert into " + tabela + " (" + colunas + ") values "
        insert = []
        for r in range(0, len(dados), step):
            row = []
            insert = sql
            for e in dados[ r : r+step ]:
                values = []
                for n in e:
                    values.append(self.limpar_dados(n))
                row.append("({})".format(', '.join(values)))
            insert += ', '.join(row)

            if self.DEBUG == 1:
                print(insert)
            else:
                try:
                    cursor.execute(insert)
                    conn.commit()
                except psycopg2.errors.UniqueViolation as e:
                    referencias = str(e).split('DETAIL')[1].split('Key')[1].split('=')[0].strip()
                    try:
                        insert_con = insert + f' ON CONFLICT {referencias} DO NOTHING'
                        cursor.execute(insert_con) # 1
                        conn.commit()
                    except:
                        conn.rollback()
                        insert_con = insert + f' ON CONFLICT DO NOTHING;'
                        cursor.execute(insert_con) # 2
                        conn.commit()
                except:
                    print(insert)
    