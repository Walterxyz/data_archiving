-- Criar tabela na base de destino dos dados (base_archiving)

CREATE TABLE public.log_archiving (
	id serial4 NOT NULL,
	tabela varchar NULL,
	datahora_inicio_archiving timestamp NULL,
	quantidade_inserida int4 NULL,
	quantidade_excluida int4 NULL,
	tamanho_tabelaorigem varchar NULL,
	tamanho_tabeladestino varchar NULL,
	tipo_alteracao varchar NULL,
	datahora_final_archiving timestamp NULL,
	tamanho_tabelaindexorigem varchar NULL,
	tamanho_tabelaindexdestino varchar NULL,
	excluido_origem bool NULL DEFAULT false,
	insert_id int4 NULL,
	CONSTRAINT log_archiving_pkey PRIMARY KEY (id)
);
CREATE INDEX log_archiving_datahora_inicio_archiving_idx ON public.log_archiving USING btree (datahora_inicio_archiving);