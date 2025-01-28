SELECT ID_тикета, 
GROUP_CONCAT(CONCAT(status_time, " " , Статус, " ", Группа) ORDER BY status_time SEPARATOR '\n') Назначение
FROM etl_3_final
GROUP BY ID_тикета