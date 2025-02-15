class PlanetaryIndustry:

    def inputs_for(self, mat):
        pass

    def suitable_planet(self, mat):
        pass


    def conversion(self, in_mat, out_mat):
        pass


    def export_tax_per_unit(self, mat):
        pass


    def sell_price(self, mat):
        pass


def p0_to_p1(pi, p1):
    p0 = pi.inputs_for(p1)["p0"][0]
    (x_planet, amt_per_day) = pi.suitable_planet(p0)

    # Process
    p1_per_day = pi.conversion(p0, p1)

    # Export
    tax_per_unit = pi.export_tax_per_unit(p1)

    # Sell
    price = pi.sell_price(p1)
    profit = price * p1_per_day - (tax_per_unit * p1_per_day)
    return profit


def p0_to_p1_to_p3(p3):
    # Extract
    # Process
    # Export
    # Import 3
    # Process
    # Process
    # Export
    # Sell


def p0_to_p2(p2):
    # Extract
    # Process
    # Process
    # Export
    # Sell


def p0_to_p3_to_p4(p4):
    # Extract
    # Process
    # Export
    # Import 3
    # Process
    # Process
    # Export 3
    # Import 4
    # Process
    # Export
    # Sell


def p0_to_p2_to_p4(p4):
    # Extract
    # Process
    # Process
    # Export
    # Import 3
    # Process
    # Process
    # Export
    # Sell
