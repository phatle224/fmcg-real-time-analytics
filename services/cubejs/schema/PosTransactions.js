cube(`PosTransactions`, {
  sql: `
    SELECT 
      from_utf8(transaction_id) as transaction_id,
      from_utf8(pos_id) as pos_id,
      from_utf8(product_id) as product_id,
      from_utf8(product_name) as product_name,
      from_utf8(category) as category,
      quantity,
      unit_price,
      total_amount,
      from_utf8(region) as region,
      from_utf8(store_type) as store_type,
      CAST(timestamp AS TIMESTAMP) as timestamp
    FROM clickhouse.default.pos_transactions_trino_view
    
    UNION ALL
    
    SELECT 
      transaction_id,
      pos_id,
      product_id,
      product_name,
      category,
      quantity,
      unit_price,
      total_amount,
      region,
      store_type,
      timestamp
    FROM iceberg.fmcg.pos_transactions_historical
  `,

  measures: {
    revenue: {
      sql: `total_amount`,
      type: `sum`
    },
    unitsSold: {
      sql: `quantity`,
      type: `sum`
    },
    transactionCount: {
      type: `count`
    },
    avgBasketSize: {
      sql: `total_amount`,
      type: `avg`
    }
  },

  dimensions: {
    region: {
      sql: `region`,
      type: `string`
    },
    category: {
      sql: `category`,
      type: `string`
    },
    storeType: {
      sql: `store_type`,
      type: `string`
    },
    productName: {
      sql: `product_name`,
      type: `string`
    },
    timestamp: {
      sql: `timestamp`,
      type: `time`
    }
  }
});
