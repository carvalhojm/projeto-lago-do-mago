SELECT DISTINCT IdProduto
FROM bronze.upsell.transacao_produto
WHERE IdProduto IS NOT NULL
ORDER BY 1