package poc.worker.tabraham;


import java.util.ArrayList;
import java.util.List;

public class Invoice {

	private String invoiceNumber;
    private List<LineItem> lineItems  = new ArrayList<LineItem>();
    private double total;
    
    

	public String getInvoiceNumber() {
		return invoiceNumber;
	}
	public void setInvoiceNumber(String invoiceNumber) {
		this.invoiceNumber = invoiceNumber;
	}
	public List<LineItem> getLineItems() {
		return lineItems;
	}
	public void setLineItems(List<LineItem> lineItems) {
		this.lineItems = lineItems;
	}
	public double getTotal() {
		return total;
	}
	public void setTotal(double total) {
		this.total = total;
	}
	public void addLineItem(LineItem lineItem) {
        this.lineItems.add(lineItem);
    }
    
	public double calculateTotals() {
        double subtotal = 0;
        for (LineItem item : lineItems) {
            subtotal += item.getPrice() * item.getQuantity();
        }
       
        this.total = subtotal;
        return subtotal;
    }
}
