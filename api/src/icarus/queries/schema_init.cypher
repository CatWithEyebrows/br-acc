// ICARUS Neo4j Schema — Constraints and Indexes
// Applied on database initialization

// -- Uniqueness Constraints --
CREATE CONSTRAINT person_cpf_unique IF NOT EXISTS
  FOR (p:Person) REQUIRE p.cpf IS UNIQUE;

CREATE CONSTRAINT company_cnpj_unique IF NOT EXISTS
  FOR (c:Company) REQUIRE c.cnpj IS UNIQUE;

CREATE CONSTRAINT contract_contract_id_unique IF NOT EXISTS
  FOR (c:Contract) REQUIRE c.contract_id IS UNIQUE;

CREATE CONSTRAINT sanction_sanction_id_unique IF NOT EXISTS
  FOR (s:Sanction) REQUIRE s.sanction_id IS UNIQUE;

CREATE CONSTRAINT public_office_cpf_unique IF NOT EXISTS
  FOR (po:PublicOffice) REQUIRE po.cpf IS UNIQUE;

CREATE CONSTRAINT investigation_id_unique IF NOT EXISTS
  FOR (i:Investigation) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT amendment_id_unique IF NOT EXISTS
  FOR (a:Amendment) REQUIRE a.amendment_id IS UNIQUE;

// -- Indexes --
CREATE INDEX person_name IF NOT EXISTS
  FOR (p:Person) ON (p.name);

CREATE INDEX person_author_key IF NOT EXISTS
  FOR (p:Person) ON (p.author_key);

CREATE INDEX person_sq_candidato IF NOT EXISTS
  FOR (p:Person) ON (p.sq_candidato);

CREATE INDEX company_razao_social IF NOT EXISTS
  FOR (c:Company) ON (c.razao_social);

CREATE INDEX contract_value IF NOT EXISTS
  FOR (c:Contract) ON (c.value);

CREATE INDEX contract_object IF NOT EXISTS
  FOR (c:Contract) ON (c.object);

CREATE INDEX sanction_type IF NOT EXISTS
  FOR (s:Sanction) ON (s.type);

CREATE INDEX election_year IF NOT EXISTS
  FOR (e:Election) ON (e.year);

CREATE INDEX election_composite IF NOT EXISTS
  FOR (e:Election) ON (e.year, e.cargo, e.uf, e.municipio);

CREATE INDEX amendment_object IF NOT EXISTS
  FOR (a:Amendment) ON (a.object);

CREATE INDEX company_cnae_principal IF NOT EXISTS
  FOR (c:Company) ON (c.cnae_principal);

CREATE INDEX contract_contracting_org IF NOT EXISTS
  FOR (c:Contract) ON (c.contracting_org);

CREATE INDEX contract_date IF NOT EXISTS
  FOR (c:Contract) ON (c.date);

CREATE INDEX sanction_date_start IF NOT EXISTS
  FOR (s:Sanction) ON (s.date_start);

CREATE INDEX amendment_date IF NOT EXISTS
  FOR (a:Amendment) ON (a.date);

// -- Fulltext Search Index --
CREATE FULLTEXT INDEX entity_search IF NOT EXISTS
  FOR (n:Person|Company)
  ON EACH [n.name, n.razao_social, n.cpf, n.cnpj];

// -- User Constraints --
CREATE CONSTRAINT user_email_unique IF NOT EXISTS
  FOR (u:User) REQUIRE u.email IS UNIQUE;
