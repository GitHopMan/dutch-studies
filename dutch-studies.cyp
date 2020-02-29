//
// Dutch Studies - HBO- en WO-opleidingen grafisch weergegeven
// Sommige tabellen zijn van het net, andere zijn zelf gemaakt
// Lars Idema, Feb. 2020
//
// Database: Neo4j, in elk geval compatible met v3.4.9 en v3.5
// Code: Cypher
//

// import opleidingen
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/CrohoNov2019.csv" AS line
CREATE (o:Opleiding {code: line.Opleidingscode, studie: line.Naam_opleiding_voluit, study: line.Internationale_naam, sector: line.Onderdeel, instelling: line.Naam_onderwijsinstelling, stad: line.Gemeentenaam, graad: line.Graad, vorm: line.Opleidingsvorm, capaciteit: line.Onderwijscapaciteit})

// Indikken: alle dubbele studies eruit
// (hou alle unieke studies per instelling over)
MATCH (p:Opleiding)
WITH p 
ORDER BY p.studie, size((p)--()) DESC
WITH p.studie as studie, p.instelling as instelling, collect(p) AS nodes 
WHERE size(nodes) >  1
UNWIND nodes[1..] AS n
DETACH DELETE n

// laad instellingen
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/onderwijsinstellingen.csv" AS line
CREATE (i:Instelling {naamInstelling: line.Naam_onderwijsinstelling})

// laad steden
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/gemeentenamen.csv" AS line
CREATE (s:Stad {naamStad: line.Gemeentenaam})

// laad sectoren
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/onderdelen.csv" AS line
CREATE (r:Sector {naamSector: line.Onderdeel})

//match de hele boel
MATCH (o:Opleiding), (i:Instelling)
WHERE o.instelling = i.naamInstelling
CREATE (o)-[r:DEELVAN]->(i)
RETURN r

//Markeer universiteiten
MATCH (n:Instelling)
WHERE ( (LOWER(n.naamInstelling) CONTAINS 'university') 
OR (LOWER(n.naamInstelling) CONTAINS 'universiteit'))
AND NOT (LOWER(n.naamInstelling) CONTAINS ('applied'))
SET n.type = 'UNIVERSITEIT'
RETURN n

//Markeer Hogescholen
MATCH (n:Instelling)
WHERE ( NOT (LOWER(n.naamInstelling) CONTAINS 'university') 
AND NOT (LOWER(n.naamInstelling) CONTAINS 'universiteit'))
OR (LOWER(n.naamInstelling) CONTAINS ('applied'))
SET n.type = 'HOGESCHOOL'
RETURN n.naamInstelling

// Voeg type UNI/HBO aan opleidingen  toe
MATCH (n:Opleiding)-[:DEELVAN]->({type: 'UNIVERSITEIT'}) 
SET n.type='WO'
// eerder: 'UNI'
RETURN n

MATCH (n:Opleiding)-[:DEELVAN]->({type: 'HOGESCHOOL'}) 
SET n.type='HBO'
RETURN n

// Markeer Bachelors en Masters
MATCH (n:Opleiding)
WHERE (n.studie STARTS WITH 'M ') 
SET n.graad = 'MASTER'
RETURN n

MATCH (n:Opleiding)
WHERE (n.studie STARTS WITH 'B ') 
SET n.graad = 'BACHELOR'
RETURN n

// je kan ook nog AD invoegen

// Markeer fixusopleidingen
MATCH (n:Opleiding)
WHERE (toInteger(n.capaciteit) > 0)
SET n.fixus="FIXUS"
RETURN n.studie, n.capaciteit

// Geef instellingen meerdere locaties, 
// Eerst: reset alle steden v/d instellingen
MATCH (i:Instelling)
SET i.steden = []
RETURN i

// Loop alle opleidingen door, en voeg die stad toe aan de instelling
MATCH (o:Opleiding),(i:Instelling)
WHERE (o.instelling = i.naamInstelling)
SET i.steden = FILTER (x IN i.steden WHERE x<>o.stad) + o.stad 
RETURN i

// Maak nu relaties tussen instellingen en steden
MATCH (i:Instelling), (s:Stad)
WHERE s.naamStad IN i.steden
CREATE (i)-[r:LIGTIN]->(s)
RETURN i

//Studiegebieden inlezen
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/gebieden.csv" AS line
CREATE (g:Gebied {naam: line.Gebied, sector: line.Sector})
RETURN g

// met studiegebieden met sectoren
MATCH (g:Gebied), (s:Sector)
WHERE (g.sector = s.naamSector)
CREATE path = (s)-[r:SUB]->(g)
RETURN path


// Studies inlezen
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/studies.csv" AS line
CREATE (t:Studie {sector: line.Sector, gebied: line.Gebied, studie: line.Studie, type: line.Type, graad: line.Graad, isat: line.ISAT, actueel: line.Actueel, historisch: line.Historisch, totaal: line.Totaal})
RETURN t

// create opleidings-type nodes, HBO Ad/HBO B/HBO M/WO B/WO M
MATCH (g:Gebied)
CREATE (g)-[:TYPE]->(:Type {type: "HBO-AD"})
CREATE (g)-[:TYPE]->(:Type {type: "HBO-B"})
CREATE (g)-[:TYPE]->(:Type {type: "HBO-M"})
CREATE (g)-[:TYPE]->(:Type {type: "WO-B"})
CREATE (g)-[:TYPE]->(:Type {type: "WO-M"})
RETURN g

// voeg studies toe aan gebieden, via type-nodes HBO Ad/HBO B/HBO M/WO B/WO M
MATCH (g:Gebied)-[:TYPE]->(t:Type {type: "HBO-AD"}), (s:Studie)
WHERE (s.type = "hbo") AND (s.studie STARTS WITH "Ad ") AND (g.naam = s.gebied)
CREATE (t)-[:STUD]->(s)
RETURN g

// voeg studies toe aan gebieden, via type-nodes HBO Ad/HBO B/HBO M/WO B/WO M
MATCH (g:Gebied)-[:TYPE]->(t:Type {type: "HBO-B"}), (s:Studie)
WHERE (s.type = "hbo") AND (s.studie STARTS WITH "B ") AND (g.naam = s.gebied)
CREATE (t)-[:STUD]->(s)
RETURN g

// voeg studies toe aan gebieden, via type-nodes HBO Ad/HBO B/HBO M/WO B/WO M
MATCH (g:Gebied)-[:TYPE]->(t:Type {type: "HBO-M"}), (s:Studie)
WHERE (s.type = "hbo") AND (s.studie STARTS WITH "M ") AND (g.naam = s.gebied)
CREATE (t)-[:STUD]->(s)
RETURN g

// voeg studies toe aan gebieden, via type-nodes HBO Ad/HBO B/HBO M/WO B/WO M
MATCH (g:Gebied)-[:TYPE]->(t:Type {type: "WO-B"}), (s:Studie)
WHERE (s.type = "wo") AND (s.studie STARTS WITH "B ") AND (g.naam = s.gebied)
CREATE (t)-[:STUD]->(s)
RETURN g

// voeg studies toe aan gebieden, via type-nodes HBO Ad/HBO B/HBO M/WO B/WO M
MATCH (g:Gebied)-[:TYPE]->(t:Type {type: "WO-M"}), (s:Studie)
WHERE (s.type = "wo") AND (s.studie STARTS WITH "M ") AND (g.naam = s.gebied)
CREATE (t)-[:STUD]->(s)
RETURN g

// match opleidingen met studies
MATCH (s:Studie), (o:Opleiding)
WHERE (s.isat = o.code)
CREATE path = (s)-[r:OPL]->(o)
RETURN path

// Delete niveaus die niet bestaan bij een studie
MATCH (g:Gebied)-[r:TYPE]-(t:Type)
WHERE NOT (t)-[:STUD]->(:Studie)
DELETE r
DELETE t
RETURN g

// Haal niet actuele studies zonder opleidingen weg
MATCH (t:Type)-[r:STUD]->(s:Studie {actueel: "."})WHERE NOT (s)-[:OPL]-(:Opleiding)
DELETE rDELETE s
RETURN t

----


SELECTIES



// Computerstudies WO-B
MATCH (n:Opleiding {type: 'WO'}) 
WHERE (LOWER(n.studie) CONTAINS "data" OR LOWER(n.studie) CONTAINS "comput" OR LOWER(n.studie) CONTAINS 'informat') AND n.graad = 'BACHELOR' 
return n.studie, n.instelling, n.fixus

----

Links laden, eerst alle WO-B opleidingen

// laden opleidingen in kopie-node
LOAD CSV WITH HEADERS FROM "https://raw.githubusercontent.com/GitHopMan/dutch-studies/master/studiekeuze123-wo-b.csv" AS line
CREATE (p:Opleid3 {link: line.Link, opleiding: line.Opleiding, instelling: line.Instelling})

// Match nu de WO-B opleidingen en breng de link aan
MATCH (o:Opleiding),(p:Opleid3)
// let niet op B_ en M_ en AD aan het begin van de studies
WHERE (o.studie CONTAINS p.opleiding) AND (o.instelling = p.instelling) AND (o.graad = "BACHELOR") AND (o.type = "WO")
SET o.link = p.link
RETURN o

Voor een aantal opleidingen is het niet gelukt de link te zetten, nl deze: (30 stuks)

MATCH (o:Opleiding)
WHERE (o.type = "WO") AND (o.graad="BACHELOR") AND NOT EXiSTS(o.link)
RETURN o.studie,o.instelling,o.link

