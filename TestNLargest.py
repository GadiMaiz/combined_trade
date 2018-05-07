import heapq
import operator

asks1 = [{'price': 9, 'amount' : 2}, {'price': 12, 'amount' : 3}, {'price': 15, 'amount' : 1}]
bids1 = [{'price': 1, 'amount' : 2}, {'price': 4, 'amount' : 3}, {'price': 7, 'amount' : 1}]

asks2 = [{'price': 10, 'amount' : 2}, {'price': 13, 'amount' : 3}, {'price': 16, 'amount' : 1}]
bids2 = [{'price': 2, 'amount' : 2}, {'price': 5, 'amount' : 3}, {'price': 8, 'amount' : 1}]

asks3 = [{'price': 11, 'amount' : 2}, {'price': 14, 'amount' : 3}, {'price': 17, 'amount' : 1}]
bids3 = [{'price': 3, 'amount' : 2}, {'price': 6, 'amount' : 3}, {'price': 9, 'amount' : 1}]

book1 = {'asks' : asks1, 'bids': bids1}
book2 = {'asks' : asks2, 'bids': bids2}
book3 = {'asks' : asks3, 'bids': bids3}



#f = heapq.nsmallest
#print(f(4, asks, key=operator.itemgetter('price')))

def get_best_orders(client_orderbooks, size):
    best_orders = [[], []]
    for curr_client in client_orderbooks:
        curr_partial_orderbook = curr_client
        index = 0
        for curr_element in [[heapq.nsmallest, curr_partial_orderbook['asks']],
                             [heapq.nlargest, curr_partial_orderbook['bids']]]:
            #for curr_element in orderbook_elements:
                # if len(best_orders[index]) < size:
                # best_orders[index].put(orderbook_elements[0](curr_element[1]['price']), orderbook_elements[1])
                best_orders[index] = curr_element[0](size, best_orders[index] + curr_element[1],
                                                     key=operator.itemgetter('price'))
                # elif best_orders[index][size - 1] < orderbook_elements[0](curr_element[1]['price']):
                index += 1

    return best_orders

print (get_best_orders([book3, book1, book2], 60))