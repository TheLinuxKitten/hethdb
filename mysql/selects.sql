
select t.blkNum,t.txIdx,t.failed from txs t join contractsCode c on t.blkNum = c.blkNum and t.txIdx = c.txIdx and hex(c.addr) = 'AA67165F85949D6D73DB62B774235FA736EE5DE6';

select hex(addr),count(*) from contractsCode where hex(code) rlike '((?i)18160ddd)' and hex(code) rlike '((?i)70a08231)' and hex(code) rlike '((?i)a9059cbb)' and hex(code) rlike '((?i)23b872dd)' and hex(code) rlike '((?i)095ea7b3)' and hex(code) rlike '((?i)dd62ed3e)' group by addr order by count(*) desc limit 10;;

select blkNum,txIdx,hex(addr) from contractsCode where hex(code) rlike '((?i)18160ddd)' and hex(code) rlike '((?i)70a08231)' and hex(code) rlike '((?i)a9059cbb)' and hex(code) rlike '((?i)23b872dd)' and hex(code) rlike '((?i)095ea7b3)' and hex(code) rlike '((?i)dd62ed3e)';

select blkNum,txIdx,hex(addr) from contractsCode where hex(code) rlike '((?i)06fdde03|95d89b41|313ce567)' and hex(code) rlike '((?i)18160ddd)' and hex(code) rlike '((?i)70a08231)' and hex(code) rlike '((?i)a9059cbb)' and hex(code) rlike '((?i)23b872dd)' and hex(code) rlike '((?i)095ea7b3)' and hex(code) rlike '((?i)dd62ed3e)';
