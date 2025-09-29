import time
import unittest
import uuid
import redis
import utils as tu


class TestMicroservices(unittest.TestCase):

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order_approved(self):
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']
        print("user_id", user_id)

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        
        self.assertIn('order_id', order)

        order_id: str = order['order_id']
        print(order_id)
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        checkout = tu.checkout_order(order_id).status_code
        print(checkout)
        time.sleep(1)

        order = tu.find_order(order_id)
        print(order)
        assert order['payment_status'] == 'approved'
        assert order['stock_status'] == 'approved'

    def test_order_declined(self):
        pass
        


    



if __name__ == '__main__':
    unittest.main()
