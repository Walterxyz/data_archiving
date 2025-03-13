import datetime
import psycopg2
import decimal

class Archiving:
    def __init__(self, dbname, user, password, host_origem, host_destino, port, qtd_registros=100):
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
        self.qtd_registros = qtd_registros
            
        self.datainicio = None
        self.step_data = 20000


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

    def refazer_ids(self, table, valores, custom_path = False):        
        schema = 'public'
        if '.' in table:
            schema = table.split('.')[0]
            table = table.split('.')[1]

        existe = self.valida_cria_tabela(table, schema)
        if not existe:
            return False
        self.path_dict = self.gera_dict_path(table, schema)
        
        if not self.path_dict and custom_path:
            self.path_dict = custom_path
    
        self.cursor_destino.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}'  and table_schema = '{schema}' order by ordinal_position ) x")
        colunas = self.cursor_destino.fetchone()[0]
        colunas = colunas.split(', ')

        self.cursor_origem.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position ) x")
        colunas_origem = self.cursor_origem.fetchone()[0]
        colunas_origem = colunas_origem.split(', ')
        colunas = ['datahora_archiving'] + [x for x in colunas if x in colunas_origem]
        colunas_str = ', '.join(colunas[1:])

        self.datainicio = datetime.datetime.now()
        self.datainicio = self.datainicio.strftime("%Y-%m-%d %H:%M:%S")
        self.cursor_origem.execute(f"select '{self.datainicio}' as datahora_archiving, {colunas_str} from {table} where id in ({valores})")
        dados = self.cursor_origem.fetchall()
        
        if len(dados) > 0:
            if self.path_dict:
                self.get_data_origem_fks(f'{schema}.{table}', dados, self.path_dict, colunas)

            self.insert_many(self.connection_destino, self.cursor_destino, dados, f'{schema}.{table}', ', '.join(colunas), 'id')
            self.log(f'{schema}.{table}', self.datainicio, 'INSERT', len(dados), 0)
        
        self.cursor_destino.close()
        self.cursor_origem.close()        
        self.connection_destino.close()
        self.connection_origem.close()
        return True

    def run(self, table, coldate, custom_path = False):
        schema = 'public'
        if '.' in table:
            schema = table.split('.')[0]
            table = table.split('.')[1]

        existe = self.valida_cria_tabela(table, schema)
        if not existe:
            return False
        self.path_dict = self.gera_dict_path(table, schema)

        if not self.path_dict and custom_path:
            self.path_dict = custom_path

        print('iniciando ' + table)
        self.cursor_destino.execute(f"select max(id) from {schema}.{table}")
        maxid = self.cursor_destino.fetchone()[0]
        if not maxid:
            maxid = 0
        
        self.cursor_destino.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position ) x")
        colunas = self.cursor_destino.fetchone()[0]
        colunas = colunas.split(', ')

        self.cursor_origem.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position ) x")
        colunas_origem = self.cursor_origem.fetchone()[0]
        colunas_origem = colunas_origem.split(', ')
        colunas = ['datahora_archiving'] + [x for x in colunas if x in colunas_origem]
        colunas_str = ', '.join(colunas[1:])

        self.datainicio = datetime.datetime.now()
        self.datainicio = self.datainicio.strftime("%Y-%m-%d %H:%M:%S")
        self.cursor_origem.execute(f"select '{self.datainicio}' as datahora_archiving, {colunas_str} from {schema}.{table} where id > {maxid} and {coldate} < CURRENT_DATE - INTERVAL '25 months' order by id limit {self.qtd_registros}")
        dados = self.cursor_origem.fetchall()

        if len(dados) > 0:
            print(f'tabela: {schema}.{table} origem: {str(len(dados))}')
            if self.path_dict:
                self.get_data_origem_fks(f'{schema}.{table}', dados, self.path_dict, colunas)

            self.insert_many(self.connection_destino, self.cursor_destino, dados, f'{schema}.{table}', ', '.join(colunas), 'id')
            self.log(f'{schema}.{table}', self.datainicio, 'INSERT', len(dados), 0)
        # print(len(dados))
        
        self.cursor_destino.close()
        self.cursor_origem.close()        
        self.connection_destino.close()
        self.connection_origem.close()
        return True        

    def log(self, tabela, datainicio, tipo_operacao, quantidade_inserida, quantidade_excluida):
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
            self.cursor_destino.execute(f"insert into public.log_archiving (datahora_inicio_archiving, datahora_final_archiving, tabela, tipo_alteracao, quantidade_inserida, quantidade_excluida, tamanho_tabelaorigem, tamanho_tabelaindexorigem, tamanho_tabeladestino, tamanho_tabelaindexdestino) values ('{datainicio}', '{datafim}', '{tabela}', '{tipo_operacao}', {quantidade_inserida}, {quantidade_excluida}, '{tamanho_tabelaorigem}', '{tamanho_tabelaindexorigem}',  '{tamanho_tabeladestino}',  '{tamanho_tabelaindexdestino}')")
            self.connection_destino.commit()
        
    # Pega recursivamente as tabelas referenciadas e insere os dados no destino
    def get_data_origem_fks(self, table, dados, path_dict, colunas):
        for tab in path_dict[table]['tabelas_ref']:
            index_refcol = colunas.index(tab['refcoluna'])
            valores = ', '.join(str(d[index_refcol]) for d in dados)
            if valores:
                self.fks_recursivo(tab['tabela'], valores, tab['coluna'], tab['refcoluna'])
                    
        return True

    def fks_recursivo(self, table, id_ref, col, refcoluna):
        schema = 'public'
        if '.' in table:
            schema = table.split('.')[0]
            table = table.split('.')[1]
        # print(table)
        existe = self.valida_cria_tabela(table, schema)
        if not existe:
            return False
        path = self.gera_dict_path(table, schema)

        self.cursor_destino.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position ) x")
        colunas = self.cursor_destino.fetchone()[0]
        colunas = colunas.split(', ')
        
        self.cursor_origem.execute(f"select string_agg(column_name, ', ') from (select column_name, ordinal_position from information_schema.columns where table_name = '{table}' and table_schema = '{schema}' order by ordinal_position ) x")
        colunas_origem = self.cursor_origem.fetchone()[0]
        colunas_origem = colunas_origem.split(', ')
        colunas = ['datahora_archiving'] + [x for x in colunas if x in colunas_origem]

        if colunas:
            colunas_str = ', '.join(colunas[1:])

            self.cursor_origem.execute(f"select count(1) from {schema}.{table} where {col} in ({id_ref})")
            l = self.cursor_origem.fetchone()[0]

            if l > 0:
                print(f'tabela: {schema}.{table} origem: {l}')

                for i in range(0, l, self.step_data):
                    if self.DEBUG == 1:
                        print(f"select '{self.datainicio}' as datahora_archiving, {colunas_str} from {schema}.{table} where {col} in ({len(id_ref.split(', '))}) order by {refcoluna} offset {i} limit {self.step_data}")
                    self.cursor_origem.execute(f"select '{self.datainicio}' as datahora_archiving, {colunas_str} from {schema}.{table} where {col} in ({id_ref}) order by {refcoluna} offset {i} limit {self.step_data}")
                    dados = self.cursor_origem.fetchall()

                    if len(dados) > 0:
                        if path:
                            # Cria a recursividade
                            self.get_data_origem_fks(f'{schema}.{table}', dados, path, colunas)
                        
                        self.insert_many(self.connection_destino, self.cursor_destino, dados, f'{schema}.{table}', ', '.join(colunas), refcoluna)
                
                self.log(f'{schema}.{table}', self.datainicio, 'INSERT', l, 0)


    def valida_cria_tabela(self, table, schema):
        self.cursor_destino.execute(f"select count(1) from pg_tables where tablename = '{table}' and schemaname = '{schema}' ")
        qtd = self.cursor_destino.fetchone()[0]
        
        if qtd > 0:
            return True
        else:
            qry = f"""
                    SELECT 'CREATE TABLE {schema}.{table} (' ||
                        ' datahora_archiving timestamp, ' ||
                        string_agg(column_name || ' ' || udt_name ||
                            CASE 
                                WHEN udt_name = 'numeric' THEN '(' || numeric_precision || ', ' || numeric_scale || ')' 
                                WHEN udt_name = 'varchar' AND character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')'
                                ELSE ''
                            END ||
                            CASE 
                                WHEN is_nullable = 'NO' THEN ' NOT NULL' 
                                ELSE ' NULL' 
                            END, ', ') ||
                        ' , CONSTRAINT ' || '{schema}_{table}_pkey PRIMARY KEY (id)' || 
                        ' )' AS createtable
                    FROM (
                        SELECT table_name, 
                            column_name, 
                            character_maximum_length, 
                            numeric_precision, 
                            numeric_scale, 
                            CASE 
                                WHEN column_name = 'id' THEN 'int4' 
                                ELSE udt_name 
                            END AS udt_name, 
                            is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}' AND table_name = '{table}'
                        ORDER BY ordinal_position
                    ) x;
                    """
            
            self.cursor_origem.execute(qry)

            create = self.cursor_origem.fetchone()

            if create :
                create = create[0]
            else:
                return False

            self.cursor_destino.execute(create)
            self.connection_destino.commit()
            return True
        
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
                print(f'inserido na tabela {tabela} {len(insert.split("), ("))} registros')
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
