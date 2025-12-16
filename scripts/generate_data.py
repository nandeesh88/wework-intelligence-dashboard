"""
WeWork Occupancy Data Generator
Generates realistic synthetic data for workspace analytics
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

class WeWorkDataGenerator:
    def __init__(self):
        self.locations = [
            'Bangalore - MG Road',
            'Mumbai - BKC', 
            'Delhi - Connaught Place',
            'Pune - Koregaon Park',
            'Hyderabad - Hitech City'
        ]
        
        self.space_types = ['Hot Desk', 'Dedicated Desk', 'Private Office', 'Meeting Room']
        
        self.member_types = ['Startup', 'Freelancer', 'Enterprise', 'SME']
        
    def generate_occupancy_data(self, days=90):
        """Generate daily occupancy data for multiple locations"""
        data = []
        start_date = datetime.now() - timedelta(days=days)
        
        for location in self.locations:
            base_occupancy = random.uniform(70, 85)
            
            for day in range(days):
                current_date = start_date + timedelta(days=day)
                day_of_week = current_date.weekday()
                
                # Weekend effect
                if day_of_week >= 5:
                    occupancy = base_occupancy * random.uniform(0.3, 0.5)
                else:
                    occupancy = base_occupancy + random.uniform(-10, 15)
                
                # Add trend
                trend = (day / days) * 5
                occupancy += trend
                
                capacity = 100
                actual_occupancy = min(occupancy, capacity)
                
                data.append({
                    'date': current_date.strftime('%Y-%m-%d'),
                    'location': location,
                    'occupancy_rate': round(actual_occupancy, 2),
                    'capacity': capacity,
                    'occupied_desks': int(actual_occupancy),
                    'day_of_week': current_date.strftime('%A')
                })
        
        return pd.DataFrame(data)
    
    def generate_hourly_data(self, days=7):
        """Generate hourly utilization data"""
        data = []
        start_date = datetime.now() - timedelta(days=days)
        
        for location in self.locations:
            for day in range(days):
                current_date = start_date + timedelta(days=day)
                
                for hour in range(9, 20):  # 9 AM to 8 PM
                    # Peak hours: 11 AM - 3 PM
                    if 11 <= hour <= 15:
                        utilization = random.uniform(80, 95)
                    elif hour in [9, 10, 16, 17]:
                        utilization = random.uniform(60, 80)
                    else:
                        utilization = random.uniform(40, 60)
                    
                    data.append({
                        'datetime': f"{current_date.strftime('%Y-%m-%d')} {hour:02d}:00",
                        'location': location,
                        'hour': hour,
                        'utilization_rate': round(utilization, 2),
                        'bookings': int(utilization * 0.8),
                        'walk_ins': int(utilization * 0.2)
                    })
        
        return pd.DataFrame(data)
    
    def generate_revenue_data(self, months=3):
        """Generate revenue data by space type"""
        data = []
        
        pricing = {
            'Hot Desk': 5000,
            'Dedicated Desk': 8000,
            'Private Office': 15000,
            'Meeting Room': 500  # per hour
        }
        
        for location in self.locations:
            for space_type in self.space_types:
                base_units = random.randint(20, 100)
                
                for month in range(months):
                    units_sold = int(base_units * random.uniform(0.8, 1.2))
                    revenue = units_sold * pricing[space_type]
                    
                    data.append({
                        'month': month + 1,
                        'location': location,
                        'space_type': space_type,
                        'units_sold': units_sold,
                        'revenue': revenue,
                        'avg_price': pricing[space_type]
                    })
        
        return pd.DataFrame(data)
    
    def generate_member_data(self, count=500):
        """Generate member demographics data"""
        data = []
        
        for i in range(count):
            join_date = datetime.now() - timedelta(days=random.randint(1, 365))
            
            member_type = random.choice(self.member_types)
            location = random.choice(self.locations)
            space_type = random.choice(self.space_types)
            
            # Tenure affects churn probability
            tenure_days = (datetime.now() - join_date).days
            churn_prob = max(0.1, 0.4 - (tenure_days / 365) * 0.2)
            is_active = random.random() > churn_prob
            
            data.append({
                'member_id': f'MEM{i+1:04d}',
                'member_type': member_type,
                'location': location,
                'space_type': space_type,
                'join_date': join_date.strftime('%Y-%m-%d'),
                'tenure_days': tenure_days,
                'is_active': is_active,
                'monthly_value': random.randint(5000, 20000)
            })
        
        return pd.DataFrame(data)
    
    def generate_all_data(self):
        """Generate all datasets and save to CSV"""
        print("🚀 Generating WeWork Analytics Data...")
        
        # Generate datasets
        occupancy_df = self.generate_occupancy_data(days=90)
        hourly_df = self.generate_hourly_data(days=7)
        revenue_df = self.generate_revenue_data(months=3)
        member_df = self.generate_member_data(count=500)
        
        # Save to CSV
        occupancy_df.to_csv('occupancy_data.csv', index=False)
        hourly_df.to_csv('hourly_utilization.csv', index=False)
        revenue_df.to_csv('revenue_data.csv', index=False)
        member_df.to_csv('member_data.csv', index=False)
        
        print("✅ Generated Files:")
        print(f"   📊 occupancy_data.csv ({len(occupancy_df)} rows)")
        print(f"   📊 hourly_utilization.csv ({len(hourly_df)} rows)")
        print(f"   📊 revenue_data.csv ({len(revenue_df)} rows)")
        print(f"   📊 member_data.csv ({len(member_df)} rows)")
        print("\n📈 Summary Statistics:")
        print(f"   Average Occupancy: {occupancy_df['occupancy_rate'].mean():.2f}%")
        print(f"   Total Revenue: ₹{revenue_df['revenue'].sum():,.0f}")
        print(f"   Active Members: {member_df['is_active'].sum()}")
        print(f"   Churn Rate: {(1 - member_df['is_active'].mean()) * 100:.1f}%")
        
        return {
            'occupancy': occupancy_df,
            'hourly': hourly_df,
            'revenue': revenue_df,
            'members': member_df
        }

if __name__ == "__main__":
    generator = WeWorkDataGenerator()
    data = generator.generate_all_data()
    print("\n✨ Data generation complete! Ready for analysis.")