MATCH (center:Company)
WHERE elementId(center) = $company_id
   OR center.cnpj = $company_identifier
   OR center.cnpj = $company_identifier_formatted
CALL apoc.path.subgraphAll(center, {
  relationshipFilter: "SOCIO_DE|VENCEU|SANCIONADA|DEVE|RECEBEU_EMPRESTIMO|BENEFICIOU|GEROU_CONVENIO|MUNICIPAL_VENCEU|MUNICIPAL_LICITOU",
  labelFilter: "+Company|+Contract|+Sanction|+Finance|+Amendment|+Convenio|+Bid|+MunicipalContract|+MunicipalBid|-Person|-Partner|-User|-Investigation|-Annotation|-Tag",
  maxLevel: $depth,
  limit: 200
})
YIELD nodes, relationships
RETURN nodes, relationships, elementId(center) AS center_id
