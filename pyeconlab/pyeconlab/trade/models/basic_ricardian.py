"""
A Basic Ricardian Trade Model 2 x 2 x 1
"""

from __future__ import division

class Country(object):
    """
    An object that represents a Country

    Parameters
    ----------
    name            :   str
                        Provide a country name
    products        :   Tuple(str, str)
                        Provide Product Names
    technology      :   Tuple(float, float)
                        Provide Technology/Productivity values where position equals product position
    """
    def __init__(self, name, products, technology):
        self.name = name
        self.products = products
        self.technology = technology
        self.exchange_rate = self.technology[0] / self.technology[1]
        #-Internal State Variables-#
        self.__output = (0,0)
        
    def __str__(self):
        try:
            units = self.units
        except:
            units = ("Unit of %s"%self.products[0], "Unit of %s"%self.products[1])
            self.units = units
        return "%s (%s - %s (Units of Labour/%s); %s - %s (Units of Labour/%s))" % (self.name, self.products[0], self.technology[0], units[0], self.products[1], self.technology[1], units[1])

    @property
    def output(self):
        return self.__output
    @output.setter
    def output(self, value):
        self.__output = value
    
    @property
    def print_output(self):
        print "%s: %s = %s; %s = %s" % (self.name, self.products[0], self.__output[0], self.products[1], self.__output[1])
    
    def set_units(self, units):
        """ 
        Set Units of Products
        
        Parameters
        ----------
        units  :  tuple(str, str)
                  Set Units of Product 1 and Product 2
        """
        self.units = units
        
    def difference_output(self, product, difference):
        """ 
        Provide a difference to the output of a given product
        
        Parameters
        ----------
        product      :  int
                        Provide Product ID 0, 1
        difference   :  numeric
                        Provide Difference in Output for the Product

        """ 
        if product == 0:
            q0 = self.__output[0] + difference
            q1 = self.__output[1] + -1 * difference * self.exchange_rate
            self.__output = (q0, q1)
        if product == 1:
            q0 = self.__output[0] + -1 * difference * 1 / self.exchange_rate
            q1 = self.__output[1] + difference
            self.__output = (q0, q1)

class TradeSystem2x2x1(object):
    """
    Constructs a 2x2x1 trading system of countries to compute various global values
    """
    def __init__(self, countries):
        self.countries = []
        for country in countries:
            print "Adding: %s" % country.name
            self.countries.append(country.name)
            #-Set Each Country as an Attribute-#
            setattr(self, country.name, country)
        #-For Now Take Products from Either Country - Assuming the Same-#
        self.products = country.products

    def __str__(self):
        global_output = self.output
        return "2x2x1 TradeSystem (Wine: %s; Cloth: %s)" % (global_output)

    @property 
    def output(self):
        output = []
        for country in self.countries: 
            output.append(getattr(self, country).output)
        q1 = output[0][0] + output[1][0]
        q2 = output[0][1] + output[1][1]
        return (q1,q2)
    
    @property
    def comparative_advantage(self):
        countries = sorted(self.countries)
        ex1 = getattr(self, countries[0]).exchange_rate    #Exchange Rate = Wine / Cloth
        ex2 = getattr(self, countries[1]).exchange_rate
        if ex1 > ex2:
            print "%s has a Comparative Advantage in %s" % (countries[0], self.products[1])
            print "%s has a Comparative Advantage in %s" % (countries[1], self.products[0])
        else:
            print "%s has a Comparative Advantage in %s" % (countries[1], self.products[0])
            print "%s has a Comparative Advantage in %s" % (countries[0], self.products[1])


if __name__ == "__main__":
    #-Differences in Output => In Line with Comparative Advantage-#
    england = Country(name="England", products=('Wine', 'Cloth'), technology=(120, 100))
    portugal = Country(name="Portugal", products=('Wine', 'Cloth'), technology=(80, 90))
    system = TradeSystem2x2x1(countries=[england, portugal])
    england.difference_output(1,6)
    england.print_output
    portugal.difference_output(1,6)
    portugal.print_output
    print system

    #-Differences in Output => Not in line with Comparative Advantage-#
    #-Construct Country Objects-#
    england = Country(name="England", products=('Wine', 'Cloth'), technology=(120, 100))
    portugal = Country(name="Portugal", products=('Wine', 'Cloth'), technology=(80, 90))
    system = TradeSystem2x2x1(countries=[england, portugal])
    england.difference_output(1,-6)
    england.print_output
    portugal.difference_output(1,6)
    portugal.print_output
    print system


