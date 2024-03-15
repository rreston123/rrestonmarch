UPDATE cs.paid_items
SET
    amount_credit = 14553.57
WHERE
        tran_no = '58907922'
    AND pay_code = 'M-CBCHG';

INSERT INTO cs.paid_items (
    tran_no,
    seq_no,
    pay_code,
    amount_credit
) VALUES (
    58907922,
    2,
    'M-VATO-S',
    1746.43
);

commit;