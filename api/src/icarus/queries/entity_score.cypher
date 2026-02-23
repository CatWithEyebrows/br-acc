// Gather exposure metrics for a given entity
MATCH (e)
WHERE elementId(e) = $entity_id
WITH e, labels(e) AS lbls
OPTIONAL MATCH (e)-[r]-(connected)
WITH e, lbls,
     count(r) AS connection_count,
     collect(DISTINCT
       CASE
         WHEN connected:Contract THEN 'transparencia'
         WHEN connected:Sanction THEN 'ceis_cnep'
         WHEN connected:Election THEN 'tse'
         ELSE 'cnpj'
       END
     ) AS source_list
OPTIONAL MATCH (e)-[:VENCEU]->(c:Contract)
WITH e, lbls, connection_count, source_list,
     COALESCE(sum(c.value), 0) AS contract_volume
OPTIONAL MATCH (e)-[:DOOU]->(d)
WITH e, lbls, connection_count, source_list, contract_volume,
     COALESCE(sum(d.amount), 0) AS donation_volume
RETURN
  elementId(e) AS entity_id,
  lbls AS entity_labels,
  connection_count,
  size(source_list) AS source_count,
  contract_volume + donation_volume AS financial_volume,
  e.cnae_principal AS cnae_principal,
  e.role AS role
