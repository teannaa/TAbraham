package poc.worker.tabraham;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class WorkerProblem {
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Enter the number of invoices: ");
		int numberOfInvoices = scanner.nextInt();
		scanner.nextLine(); 

		List<Invoice> invoices = new ArrayList<>();
		for (int i = 0; i < numberOfInvoices; i++) {
			Invoice invoice = new Invoice();
			invoice.setInvoiceNumber(generateUniqueInvoiceNumber());
			System.out.println("Enter details for Invoice " + (i + 1));
			System.out.print("Enter the number of line items: ");
			int numberOfLineItems = scanner.nextInt();
			scanner.nextLine(); 
			
			List<LineItem> lineItems = new ArrayList<>();
			for (int j = 0; j < numberOfLineItems; j++) {
				LineItem lineItem = new LineItem();
				System.out.println("Enter details for Line Item " + (j + 1));
				System.out.print("Description: ");
				lineItem.setDescription(scanner.nextLine());
				System.out.print("Price: ");
				lineItem.setPrice(scanner.nextDouble());
				System.out.print("Quantity: ");
				lineItem.setQuantity(scanner.nextInt());
				scanner.nextLine(); 
				lineItems.add(lineItem);
			}
			invoice.setLineItems(lineItems);
			invoices.add(invoice);
		}

		System.out.println("Select the type of worker:");
		System.out.println("1. Sequential");
		System.out.println("2. Concurrent");
		System.out.println("3. Async");
		int workerType = scanner.nextInt();

		Worker<Invoice> worker = null;
		switch (workerType) {
		case 1:
			worker = Worker.sequential(Invoice.class).step()
					.action(data -> System.out.println("Step 1: Obtained invoice number " + data.getInvoiceNumber()))
					.retries(3).optional().add().step()
					.action(data -> System.out.println("Step 2: Added line items to the invoice")).add().step()
					.action(data -> {
						System.out.println("Step 3: Calculated taxes and totals for the invoice");
						System.out.println(data.calculateTotals());
					}).rollback(data -> System.out.println("Step 3 rollback: Rolled back tax and total calculation"))
					.add().build();
			break;
		case 2:
			worker = Worker.concurrent(Invoice.class).threads(10).step().action(data -> {
				
			}).add().step()
					.action(data -> System.out.println("Step 1: Obtained invoice number " + data.getInvoiceNumber()))
					.retries(3).optional().add().step()
					.action(data -> System.out.println("Step 2: Added line items to the invoice")).add().step()
					.action(data -> {
						System.out.println("Step 3: Calculated taxes and totals for the invoice");
						System.out.println(data.calculateTotals());
					}).rollback(data -> System.out.println("Step 3 rollback: Rolled back tax and total calculation"))
					.add().build();

			break;
		case 3:
			worker = Worker.async(Invoice.class).threads(10).step().action(data -> {
				
			}).add().step().action(data -> {
				
			}).add().build();
			break;
		default:
			System.out.println("Invalid option. Using sequential worker by default.");
			worker = Worker.sequential(Invoice.class).step()
					.action(data -> System.out.println("Step 1: Obtained invoice number " + data.getInvoiceNumber()))
					.retries(3).optional().add().step()
					.action(data -> System.out.println("Step 2: Added line items to the invoice")).add().step()
					.action(data -> {
						data.calculateTotals();
						System.out.println("Step 3: Calculated taxes and totals for the invoice");
					}).rollback(data -> System.out.println("Step 3 rollback: Rolled back tax and total calculation"))
					.add().build();
			break;
		}

		worker.run(invoices);

		scanner.close();
	}

	public static String generateUniqueInvoiceNumber() {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
		String formattedDateTime = now.format(formatter);
		return "INV_" + formattedDateTime;
	}
}
