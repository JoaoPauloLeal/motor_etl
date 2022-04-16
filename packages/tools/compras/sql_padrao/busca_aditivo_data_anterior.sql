SELECT c.i_contratos as col,
    c.nr_contrato as contrato_original,
	c.data_ass as data_assinatura,
	c2.nr_contrato as contrato_aditivo,
	c2.data_ass as data_assinatura_aditivo
from compras.contratos c left join compras.contratos c2
	on c.nr_contrato = c2.nr_contrato_sup
		and c.i_entidades = c2.i_entidades
where c.nr_contrato_sup is NULL
	and c.i_entidades = {{entidade}}
	and c.data_ass > c2.data_ass
ORDER BY c.nr_contrato, c2.nr_contrato ;
