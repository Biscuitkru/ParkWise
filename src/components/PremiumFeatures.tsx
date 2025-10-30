import { useState } from 'react';
import { 
  Crown, 
  Calculator, 
  Bell, 
  Target
} from 'lucide-react';
import { Button } from './ui/button';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import { Badge } from './ui/badge';
import { Input } from './ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Slider } from './ui/slider';
import { useCarparks } from '../hooks/useCarparks';

interface PremiumFeaturesProps {
  isPremium: boolean;
  onViewChange: (view: string) => void;
}

export function PremiumFeatures({ isPremium, onViewChange }: PremiumFeaturesProps) {
  const [parkingDuration, setParkingDuration] = useState([2]);
  
  // Fetch carparks from the API
  const { carparks: mockCarparks, loading } = useCarparks();
  const [selectedCarpark, setSelectedCarpark] = useState(mockCarparks[0]?.id || '');

  if (!isPremium) {
    return (
      <div className="h-full overflow-y-auto">
        <div className="max-w-4xl mx-auto p-3 sm:p-4">
        <Card className="text-center p-6 sm:p-8 border-yellow-200 bg-yellow-50 dark:bg-yellow-950 dark:border-yellow-800">
          <Crown className="w-12 h-12 sm:w-16 sm:h-16 text-yellow-600 dark:text-yellow-400 mx-auto mb-4" />
          <h2 className="text-xl sm:text-2xl mb-4">Premium Features</h2>
          <p className="text-muted-foreground mb-6 text-sm sm:text-base">
            Upgrade to Premium to access smart recommendations, cost calculator, 
            and waitlist notifications.
          </p>
          <Button onClick={() => onViewChange('pricing')} className="w-full sm:w-auto">
            Upgrade to Premium - S$3.99/month
          </Button>
        </Card>
        </div>
      </div>
    );
  }

  const selectedCarparkData = mockCarparks.find(cp => cp.id === selectedCarpark);
  const totalCost = selectedCarparkData ? 
    (selectedCarparkData.rates.hourly * parkingDuration[0]) : 0;

  return (
    <div className="h-full overflow-y-auto">
      <div className="max-w-6xl mx-auto p-3 sm:p-4">
      <div className="flex flex-col sm:flex-row sm:items-center gap-3 mb-4 sm:mb-6">
        <div className="flex items-center gap-3">
          <Crown className="w-5 h-5 sm:w-6 sm:h-6 text-yellow-600" />
          <h1 className="text-xl sm:text-2xl">Premium Features</h1>
        </div>
        <Badge className="bg-yellow-100 text-yellow-800 w-fit">Active</Badge>
      </div>

      <Tabs defaultValue="calculator" className="space-y-4 sm:space-y-6">
        <TabsList className="grid w-full grid-cols-3 h-auto">
          <TabsTrigger value="calculator" className="text-xs sm:text-sm">Calculator</TabsTrigger>
          <TabsTrigger value="recommender" className="text-xs sm:text-sm">Recommender</TabsTrigger>
          <TabsTrigger value="notifications" className="text-xs sm:text-sm">Alerts</TabsTrigger>
        </TabsList>

        {/* Cost Calculator */}
        <TabsContent value="calculator">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Calculator className="w-5 h-5" />
                  Parking Cost Calculator
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <label className="text-sm mb-2 block">Select Carpark</label>
                  <Select value={selectedCarpark} onValueChange={setSelectedCarpark}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {mockCarparks.map(carpark => (
                        <SelectItem key={carpark.id} value={carpark.id}>
                          {carpark.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <label className="text-sm mb-2 block">
                    Parking Duration: {parkingDuration[0]} hours
                  </label>
                  <Slider
                    value={parkingDuration}
                    onValueChange={setParkingDuration}
                    max={12}
                    min={0.5}
                    step={0.5}
                    className="w-full"
                  />
                </div>


              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Cost Breakdown</CardTitle>
              </CardHeader>
              <CardContent>
                {selectedCarparkData && (
                  <div className="space-y-3">
                    <div className="flex justify-between">
                      <span>Parking ({parkingDuration[0]}h)</span>
                      <span>S${(selectedCarparkData.rates.hourly * parkingDuration[0]).toFixed(2)}</span>
                    </div>

                    <div className="border-t pt-2">
                      <div className="flex justify-between text-lg">
                        <span>Total Cost</span>
                        <span>S${totalCost.toFixed(2)}</span>
                      </div>
                    </div>

                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Smart Recommender */}
        <TabsContent value="recommender">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Target className="w-5 h-5" />
                Smart Carpark Recommendations
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {mockCarparks.slice(0, 3).map((carpark, index) => {
                  const reasons = [
                    index === 0 ? 'Best value for money' : '',
                    index === 1 ? 'Closest to destination' : '',
                    index === 2 ? 'Highest availability' : ''
                  ].filter(Boolean);

                  return (
                    <Card key={carpark.id} className="relative">
                      {index === 0 && (
                        <Badge className="absolute -top-2 left-4 bg-green-600">
                          Recommended
                        </Badge>
                      )}
                      <CardHeader className="pb-2">
                        <CardTitle className="text-lg">{carpark.name}</CardTitle>
                        <div className="text-sm text-muted-foreground">
                          {reasons.join(', ')}
                        </div>
                      </CardHeader>
                      <CardContent>
                        <div className="space-y-2 text-sm">
                          <div className="flex justify-between">
                            <span>Distance</span>
                            <span>{carpark.distance !== undefined ? `${carpark.distance}km` : '-'}</span>
                          </div>
                          <div className="flex justify-between">
                            <span>Rate</span>
                            <span>S${carpark.rates.hourly}/30min</span>
                          </div>
                          <div className="flex justify-between">
                            <span>Availability</span>
                            <span>{carpark.availableLots}/{carpark.totalLots !== null ? carpark.totalLots : 'N/A'}</span>
                          </div>
                        </div>
                        <Button className="w-full mt-3" size="sm">
                          Select This Carpark
                        </Button>
                      </CardContent>
                    </Card>
                  );
                })}
              </div>
            </CardContent>
          </Card>
        </TabsContent>



        {/* Notifications */}
        <TabsContent value="notifications">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Bell className="w-5 h-5" />
                  Availability Alerts
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <label className="text-sm mb-2 block">Carpark</label>
                  <Select>
                    <SelectTrigger>
                      <SelectValue placeholder="Select a carpark" />
                    </SelectTrigger>
                    <SelectContent>
                      {mockCarparks.map(carpark => (
                        <SelectItem key={carpark.id} value={carpark.id}>
                          {carpark.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <label className="text-sm mb-2 block">Alert when availability is above</label>
                  <div className="flex items-center gap-2">
                    <Input type="number" defaultValue="20" className="w-20" />
                    <span className="text-sm">lots available</span>
                  </div>
                </div>

                <Button className="w-full">
                  Set Availability Alert
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Active Notifications</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="p-3 bg-green-50 rounded-lg border border-green-200">
                    <div className="flex items-center justify-between">
                      <div>
                        <div className="text-sm">Marina Bay Sands</div>
                        <div className="text-xs text-muted-foreground">Alert when &gt;30 lots available</div>
                      </div>
                      <Badge className="bg-green-600">Active</Badge>
                    </div>
                  </div>

                  <div className="p-3 bg-yellow-50 rounded-lg border border-yellow-200">
                    <div className="flex items-center justify-between">
                      <div>
                        <div className="text-sm">ION Orchard</div>
                        <div className="text-xs text-muted-foreground">Waitlist for EV charging</div>
                      </div>
                      <Badge className="bg-yellow-600">Waitlist</Badge>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>
      </Tabs>
      </div>
    </div>
  );
}