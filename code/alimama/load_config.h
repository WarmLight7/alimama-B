#include <vector>
#include <unordered_map>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

#include "csv.h"

// Define data structures for storing the parsed data
struct PromotionalPlan {
    uint64_t campaign_id;
    int64_t status;
    std::vector<int64_t> timings;
};

struct PromotionalKeywordPrice {
    uint64_t keyword;
    uint64_t price;
};

struct PromotionalUnit {
    uint64_t adgroup_id;
    uint64_t campaign_id;
    std::vector<PromotionalKeywordPrice> keyword_prices;
    uint64_t item_id;
};

struct ProductVectorKeyword {
    uint64_t keyword;
    std::vector<float> vector;
};

using ProductVectorsKeywordMap = std::unordered_map<uint64_t, ProductVectorKeyword>;

struct ProductVector {
    uint64_t item_id;
    ProductVectorsKeywordMap keywords;
};


using PromotionalPlansMap = std::unordered_map<uint64_t, PromotionalPlan>;
using PromotionalUnitsMap = std::unordered_map<uint64_t, PromotionalUnit>;
using ProductVectorsMap = std::unordered_map<uint64_t, ProductVector>;

std::vector<int64_t> parseInt64List(const std::string& s) {
    std::vector<int64_t> result;
    std::istringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(std::stoull(item));
    }
    return result;
}

std::vector<PromotionalKeywordPrice> parseKeywordPriceList(const std::string& s) {
    std::vector<PromotionalKeywordPrice> result;
    std::istringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        std::istringstream ss_kw_price(item);
        std::string kw{};
        std::string price{};
        std::getline(ss_kw_price, kw, ':');
        std::getline(ss_kw_price, price, ':');
        result.push_back(PromotionalKeywordPrice{
            std::stoull(kw),
            std::stoull(price)
        });
    }
    return result;
}

std::vector<uint64_t> parseUint64List(const std::string& s) {
    std::vector<uint64_t> result;
    std::istringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(std::stoull(item));
    }
    return result;
}

std::vector<float> parseFloatList(const std::string& s) {
    std::vector<float> result;
    std::istringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(std::stof(item));
    }
    return result;
}

int loadconfig(PromotionalPlansMap& promotionalPlans, PromotionalUnitsMap& promotionalUnits,
        ProductVectorsMap& productVectors) {
    // Read promotional_plans.csv
    {
        csv::CSVReader<3, csv::trim_chars<' '>, csv::no_quote_escape<'\t'>> in("/data/promotional_plans.csv");
        in.read_header(csv::ignore_extra_column, "campaign_id", "status", "timings");
        uint64_t campaign_id;
        int8_t status;
        std::string timings;
        while (in.read_row(campaign_id, status, timings)) {
            PromotionalPlan plan;
            plan.campaign_id = campaign_id;
            plan.status = status;
            plan.timings = parseInt64List(timings);
            promotionalPlans[campaign_id] = plan;
        }
    }

    // Read promotional_units.csv
    {
        csv::CSVReader<4, csv::trim_chars<' '>, csv::no_quote_escape<'\t'>> in("/data/promotional_units.csv");
        in.read_header(csv::ignore_extra_column, "adgroup_id", "campaign_id", "keyword_prices", "item_id");
        uint64_t adgroup_id, campaign_id, item_id;
        std::string keyword_prices;
        while (in.read_row(adgroup_id, campaign_id, keyword_prices, item_id)) {
            PromotionalUnit unit;
            unit.adgroup_id = adgroup_id;
            unit.campaign_id = campaign_id;
            unit.keyword_prices = parseKeywordPriceList(keyword_prices);
            unit.item_id = item_id;
            promotionalUnits[adgroup_id] = unit;
        }
    }

    // Read product_vectors.csv
    {
        csv::CSVReader<3, csv::trim_chars<' '>, csv::no_quote_escape<'\t'>> in("/data/product_vectors.csv");
        in.read_header(csv::ignore_extra_column, "item_id", "keyword", "vector");
        uint64_t item_id, keyword;
        std::string vector;
        while (in.read_row(item_id, keyword, vector)) {
            ProductVector product;
            product.item_id = item_id;


            if (productVectors.find(item_id) == productVectors.end()) {
                productVectors.insert({item_id, product});
            }

            productVectors[item_id].keywords.insert({keyword, 
                ProductVectorKeyword{keyword, parseFloatList(vector)} });
        }
    }
    // Access the data in the maps
    // You can perform operations on the data stored in the maps here
    // For example, you can iterate over the maps to print the data

    // Print promotional plans
    std::cout << "Promotional Plans:\n";
    for (const auto& entry : promotionalPlans) {
        const PromotionalPlan& plan = entry.second;
        std::cout << "Campaign ID: " << plan.campaign_id << ", Status: " << plan.status << ", Timings: ";
        for (int8_t timing : plan.timings) {
            std::cout << timing << " ";
        }
        std::cout << "\n";
    }

    // Print promotional units
    std::cout << "Promotional Units:\n";
    for (const auto& entry : promotionalUnits) {
        const PromotionalUnit& unit = entry.second;
        std::cout << "Adgroup ID: " << unit.adgroup_id << ", Campaign ID: " << unit.campaign_id << ", Keyword Prices: ";
        for (const auto& kw_price : unit.keyword_prices) {
            std::cout << " keyword " << kw_price.keyword << " price " << kw_price.price;
        }
        std::cout << ", Item ID: " << unit.item_id << "\n";
    }

    // Print product vectors
    std::cout << "Product Vectors:\n";
    for (const auto& entry : productVectors) {
        const ProductVector& product = entry.second;
        for (const auto& entry_kw : product.keywords) {
            std::cout << "Item ID: " << product.item_id << ", Keyword: " << entry_kw.second.keyword << ", Vector: ";
            for (float value : entry_kw.second.vector) {
                std::cout << value << " ";
            }
            std::cout << "\n";
        }
    }

    return 0;
}

