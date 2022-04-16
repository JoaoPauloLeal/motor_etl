SELECT c.i_contratos as col,
    c.nr_contrato as contrato_original,
	count(c2.data_ass) as ocor,
	c2.nr_contrato as contrato_aditivo,
	c2.data_ass as data_assinatura_aditivo
from compras.contratos c left join compras.contratos c2
	on c.nr_contrato = c2.nr_contrato_sup
		and c.i_entidades = c2.i_entidades
where c.nr_contrato_sup is NULL
	and c.i_entidades = {{entidade}}
GROUP BY c2.nr_contrato, c.nr_contrato , c.data_ass , c2.data_ass,  c.i_contratos
HAVING ocor > 1
ORDER BY c.nr_contrato, c2.nr_contrato ;