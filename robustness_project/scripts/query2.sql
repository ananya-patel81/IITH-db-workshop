SELECT COUNT(*)
FROM title t
JOIN ratings r ON t.tconst = r.tconst
WHERE r.averagerating >= 7.0
AND t.startyear >= {};
