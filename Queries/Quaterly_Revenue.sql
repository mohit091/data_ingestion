select
  distinct concat(
    concat(
      extract(
        year
        from
          date
      ),
      '-Q'
    ),
    extract(
      quarter
      from
        date
    )
  ) as quarter,
  total_amount
from
  (
    select
      CURRENT_DATE + i as date
    from
      generate_series(
        date '2020-01-01' - CURRENT_DATE, date '2021-06-01' - CURRENT_DATE
      ) i
  ) a
  left join (
    select
      concat(
        concat(
          extract(
            year
            from
              transaction_date
          ),
          '-Q'
        ),
        extract(
          quarter
          from
            transaction_date
        )
      ) as transaction_quarter,
      sum(total_amount) as total_amount
    from
      TRANSACTIONS.FACT_TRANSACTION
    where
      transaction_type = 1
    group by
      1
  ) ft on concat(
    concat(
      extract(
        year
        from
          date
      ),
      '-Q'
    ),
    extract(
      quarter
      from
        date
    )
  )= transaction_quarter;


