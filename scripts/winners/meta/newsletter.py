def newsletter():
    import pandas as pd

    df = pd.read_csv('/home/tier1marketspace/youtuberReport/scripts/data/winners_final.csv')

    # Name, SetNumber, Price 2, $ change, % Change, TCG url, IMAGE url

    # List of columns you want to keep
    selected_columns = ['Name', 'Set Number', 'Price (2)','Change ($)','Change (%)','imageUrl','tcgplayerUrl','Period','Edition','Rarity']  # Replace with the names of the columns you want

    # Create a new DataFrame with only those columns
    df = df[selected_columns]

    df = df.drop_duplicates()

    # 1) Remove all rows with a period that is not 7
    df = df[df['Period'] == 7]

    # 2) Remove any row with a name that includes "starlight"
    df = df[~df['Name'].str.contains("starlight", case=False)]

    # 3) Format 'Price (2)' as $
    df['Price (2)'] = df['Price (2)'].apply(lambda x: f"${x}")

    # 4) Format 'Change (%)' as %
    df['Change (%)'] = df['Change (%)'].apply(lambda x: f"{x * 100:.2f}%")  # Multiply by 100 and round to 2 decimal places

    # 5) Remove all rows with 'Edition' that is 'Unlimited'
    df = df[~((df['Edition'] == 'Unlimited') & (df['Rarity'] != 'Ultimate Rare'))]


    # 6) Sort by 'Change ($)' in descending order
    df = df.sort_values('Change ($)', ascending=False)

    # 3.5) Format 'Change ($)' as $
    df['Change ($)'] = df['Change ($)'].apply(lambda x: f"${x}")

    # Display the DataFrame to check if everything worked
    print(df)

    df.to_csv('/home/tier1marketspace/discordBot/newsletter.csv', index=False)
newsletter()