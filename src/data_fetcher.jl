module DataFetcher

using Dates
using HTTP
using JSON
using DataFrames
using CSV

export fetch_data, save_data, load_data

"""
fetch historical price data from yahoo finance for a given ticker and date range.
    parameters:
    - ticker: stock symbol (default: "^GSPC" for S&P 500)
    - start_date: start date (default: 5 years ago)
    - end_date: end date (default: today)
    - interval: data interval (e.g, 1d, 1wk, 1mo, default: 1d)
returns a dataframe with historical price data.
"""
function fetch_data(;
    ticker::String = "^GSPC",
    start_date::Union{Date, DateTime} = Dates.today() - Dates.Year(5),
    end_date::Union{Date, DateTime} = Dates.today(),
    interval::String = "1d"
)
    # change Date to DateTime
    start_dt = start_date isa Date ? DateTime(start_date) : start_date
    end_dt = end_date isa Date ? DateTime(end_date) : end_date

    # change Datetime to unix time
    start_date_unix = Int(floor(datetime2unix(start_dt)))
    end_date_unix = Int(floor(datetime2unix(end_dt)))

    # create url to data
    base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"
    params = "?period1=$(start_date_unix)&period2=$(end_date_unix)&interval=$(interval)&events=history"
    url = base_url * HTTP.escapeuri(ticker) * params

    try
        # make http request (with user agent to make requests more reliable)
        response = HTTP.get(url, headers=Dict("User-Agent" => "Mozilla/5.0"))

        # parse json response
        data = JSON.parse(String(response.body))

        # extract data
        result = data["chart"]["result"][1]
        timestamps = result["timestamp"]
        quote_data = result["indicators"]["quote"][1]

        # create data frame
        df = DataFrame(
            date = unix2datetime.(timestamps),
            open = get(quote_data, "open", fill(missing, length(timestamps))),
            high = get(quote_data, "high", fill(missing, length(timestamps))),
            low = get(quote_data, "low", fill(missing, length(timestamps))),
            close = get(quote_data, "close", fill(missing, length(timestamps))),
            volume = get(quote_data, "volume", fill(missing, length(timestamps)))
        )

        # handle any missing values
        df = dropmissing(df, :close)

        # sort by date with most recent data on top
        sort!(df, :date, rev = true)

        return df
    catch er
        error("Error fetching data: $(sprint(showerror, er))")
    end
end

"""
save data to a csv file.
"""
function save_data(
    df::DataFrame,
    ticker::String;
    path::String = "./data"
)
    # ensure directory exists
    mkpath(path)
    
    # create filename with ticker and date range
    filename = joinpath(path, "$(ticker)_$(Dates.format(minimum(df.date), "yyyymmdd"))_$(Dates.format(maximum(df.date), "yyyymmdd")).csv")

    try
        # write to file
        CSV.write(filename, df)
        
        return filename
    catch er
        error("Error saving data: $(sprint(showerror, er))")
    end
end

"""
load data from a csv file.
"""
function load_data(filename::String)
    try
        # read from file
        df = CSV.read(filename, DataFrame)
        
        # ensure date column is DateTime
        if :date in names(df) && eltype(df.date) <: AbstractString
            df.date = DateTime.(df.date)
        end

        return df
    catch er
        error("Error loading data: $(sprint(showerror, er))")
    end
end

end