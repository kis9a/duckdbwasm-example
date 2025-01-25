import { h } from "preact";
import { LocationProvider, Router, Route } from "preact-iso";
import { Header } from "@/components/header";
import Home from "@/pages/home";
import About from "@/pages/about";

export function App() {
  return (
    <LocationProvider>
      <div class="fixed bottom-4 right-2 mr-2">
        <Header />
      </div>
      <Router>
        <Route path="/" component={Home} />
        <Route path="/about" component={About} />
      </Router>
    </LocationProvider>
  );
}
