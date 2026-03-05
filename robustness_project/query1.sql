SELECT COUNT(*)
FROM title t
JOIN cast_info c ON t.tconst = c.tconst
JOIN name n ON c.nconst = n.nconst
WHERE t.startyear >= {};
