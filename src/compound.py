def Compound(df, apport, period, asset):
    counter = 0
    total = 0
    # change type as float
    df[asset] = df[asset].astype(float)
    lastPrice = df.iloc[0][asset]
    df['Compound'] = df[asset]
    for index, row in df.iterrows():
        total = row[asset] * total / lastPrice
        if period == 'd':
            total += apport
        elif period == 'w' and counter % 7 == 0:
            total += apport
        elif period == 'm' and counter % 30 == 0:
            total += apport
        elif period == 'y' and counter % 365 == 0:
            total += apport
        counter += 1
        lastPrice = row[asset]
        row['Compound'] = total
    return df
