/****************************************************************************************
 2. Split a product’s product_code value into four separate columns.
******************************************************************************************/
select
    product_id,
    split(product_code, '_')[safe_offset(0)] as country,
    split(product_code, '_')[safe_offset(1)] as vertical,
    split(product_code, '_')[safe_offset(2)] as product_type,
    split(product_code, '_')[safe_offset(3)] as vehicle_type
from product
