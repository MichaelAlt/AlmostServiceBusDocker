namespace OrderFlowDemo.OrderApi.Scenarios;

public static class OrderDataGenerator
{
    private static readonly string[] Customers =
    [
        "Arthur Dent", "Ford Prefect", "Trillian Astra", "Zaphod Beeblebrox",
        "Marvin the Paranoid", "Slartibartfast", "Prostetnic Vogon Jeltz",
        "Deep Thought", "Eddie the Shipboard Computer", "Agrajag",
        "Fenchurch", "Random Dent", "Humma Kavula", "Questular Rontok",
    ];

    private static readonly (string Name, decimal MinPrice, decimal MaxPrice)[] Products =
    [
        ("Pan Galactic Gargle Blaster", 42.00m, 142.00m),
        ("Babel Fish", 5.99m, 15.99m),
        ("Towel (Extra Fluffy)", 12.50m, 35.00m),
        ("Infinite Improbability Drive", 299.99m, 499.99m),
        ("Point-of-View Gun", 75.00m, 150.00m),
        ("Sub-Etha Sens-O-Matic", 49.99m, 89.99m),
        ("Nutrimatic Cup of Tea", 3.50m, 8.99m),
        ("Joo Janta 200 Peril Sensitive Sunglasses", 65.00m, 120.00m),
        ("Electronic Thumb", 29.99m, 59.99m),
        ("Bistromathic Drive", 350.00m, 500.00m),
        ("Heart of Gold Model Kit", 19.99m, 45.00m),
        ("Guide Mark II", 199.99m, 399.99m),
        ("Dish of the Day Menu", 8.99m, 22.00m),
        ("Vogon Poetry Anthology", 1.99m, 4.99m),
        ("Kill-O-Zap Blaster Pistol", 89.99m, 189.99m),
        ("Magrathean Custom Planet Voucher", 250.00m, 500.00m),
    ];

    private static readonly string[] Warehouses =
        ["London-East", "Manchester", "Birmingham", "Edinburgh"];

    private static readonly Random Rng = Random.Shared;

    public static string RandomCustomer() => Customers[Rng.Next(Customers.Length)];

    public static string RandomWarehouse() => Warehouses[Rng.Next(Warehouses.Length)];

    public static (string[] Names, decimal TotalAmount) RandomProducts()
    {
        var count = Rng.Next(1, 6);
        var names = new string[count];
        var total = 0m;

        for (var i = 0; i < count; i++)
        {
            var product = Products[Rng.Next(Products.Length)];
            names[i] = product.Name;
            total += Math.Round(
                product.MinPrice + (decimal)Rng.NextDouble() * (product.MaxPrice - product.MinPrice),
                2);
        }

        return (names, total);
    }
}
